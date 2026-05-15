import os
import json
import uuid
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
from bson import ObjectId
from datetime import datetime
from user_activity_graph import (
    log_view_restaurant,
    log_order_food,
    log_review
)


# ============================================================
# KẾT NỐI
# ============================================================
db   = MongoClient("mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin")["food_delivery"]
cass = Cluster(["localhost"], port=9042).connect("food_delivery")
r    = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

cart = []
current_customer = None

# ============================================================
# HELPER
# ============================================================
def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def divider(title=""):
    if title:
        print(f"\n{'='*55}")
        print(f"  {title}")
        print(f"{'='*55}")
    else:
        print("-"*55)

def pause():
    input("\n  [Nhấn Enter để tiếp tục]")

def pick_customer():
    global current_customer
    customers = list(db.customers.find({"status": "active"}).limit(5))
    clear()
    divider("CHỌN KHÁCH HÀNG (demo)")
    for i, c in enumerate(customers, 1):
        print(f"  {i}. {c['full_name']} | {c['phone']}")
    while True:
        try:
            ch = int(input("\n  Chọn số: "))
            if 1 <= ch <= len(customers):
                current_customer = customers[ch-1]
                return
        except ValueError:
            pass
        print("  Vui lòng nhập số hợp lệ.")

# ============================================================
# REDIS HELPERS
# ============================================================

def get_menu_from_cache(restaurant_id, restaurant_oid):
    """
    [REDIS] Lấy menu từ cache.
    Cache miss → đọc MongoDB → lưu Redis TTL 300s
    """
    cache_key = f"menu:{restaurant_id}"
    cached = r.get(cache_key)
    if cached:
        print("  📦 [Redis] Cache HIT — menu từ cache (không query MongoDB)")
        return json.loads(cached)
    else:
        print("  📭 [Redis] Cache MISS — đọc từ MongoDB, lưu cache TTL 300s")
        items = list(db.menu_items.find(
            {"restaurant_id": restaurant_oid, "is_available": True},
            {"name": 1, "price": 1, "description": 1, "category": 1}
        ))
        # Serialize ObjectId trước khi lưu
        for item in items:
            item["_id"] = str(item["_id"])
            item["restaurant_id"] = str(restaurant_oid)
        r.set(cache_key, json.dumps(items), ex=300)
        return items


def cache_order_status(order):
    """[REDIS] Cache trạng thái đơn hàng real-time, TTL 2 giờ"""
    key = f"order:status:{str(order['_id'])}"
    r.hset(key, mapping={
        "status":        order["current_status"],
        "updated_at":    order["updated_at"].isoformat(),
        "customer_id":   str(order["customer_id"]),
        "restaurant_id": str(order["restaurant_id"]),
    })
    r.expire(key, 7200)
    print(f"  🔴 [Redis] Đã cache trạng thái đơn | TTL: 7200s")


def get_order_status_from_cache(order_id):
    """[REDIS] Lấy trạng thái đơn từ Redis cache"""
    key = f"order:status:{order_id}"
    data = r.hgetall(key)
    if data:
        ttl = r.ttl(key)
        print(f"  🔴 [Redis] Trạng thái từ cache | TTL còn: {ttl}s")
        return data
    return None


def log_activity_cassandra(event_type, restaurant_id=None, menu_item_id=None):
    """
    [CASSANDRA] Ghi activity log — append-only, time-series
    Không dùng MongoDB vì volume cực lớn, write-heavy
    """
    insert_stmt = cass.prepare("""
        INSERT INTO customer_activity
        (customer_id, timestamp, event_id, event_type, restaurant_id, menu_item_id, search_keyword)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    cid = uuid.uuid5(uuid.NAMESPACE_OID, str(current_customer["_id"]))
    rid = uuid.uuid5(uuid.NAMESPACE_OID, str(restaurant_id)) if restaurant_id else None
    mid = uuid.uuid5(uuid.NAMESPACE_OID, str(menu_item_id))  if menu_item_id  else None

    cass.execute(insert_stmt, (
        cid, datetime.now(), uuid.uuid4(),
        event_type, rid, mid, None
    ))
    print(f"  📊 [Cassandra] Activity logged: {event_type}")


def get_order_history_cassandra():
    """
    [CASSANDRA] Lấy lịch sử đơn hàng — partition by customer_id
    Tối ưu time-series, sort sẵn theo thời gian
    """
    cid  = uuid.uuid5(uuid.NAMESPACE_OID, str(current_customer["_id"]))
    rows = list(cass.execute("""
        SELECT order_id, created_at, restaurant_name,
               current_status, total_amount, payment_method, item_summary
        FROM order_history_by_customer
        WHERE customer_id = %s
        LIMIT 20
    """, (cid,)))
    print(f"  📊 [Cassandra] Lấy lịch sử đơn hàng — {len(rows)} records")
    return rows


def sync_order_to_cassandra(order, restaurant_name):
    """[CASSANDRA] Đồng bộ đơn mới vào order_history"""
    insert_stmt = cass.prepare("""
        INSERT INTO order_history_by_customer
        (customer_id, created_at, order_id, restaurant_name,
         current_status, total_amount, payment_method, item_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    cid     = uuid.uuid5(uuid.NAMESPACE_OID, str(order["customer_id"]))
    oid     = uuid.uuid5(uuid.NAMESPACE_OID, str(order["_id"]))
    summary = ", ".join([f"{i['name']} x{i['quantity']}"
                         for i in order["items"][:3]])
    cass.execute(insert_stmt, (
        cid, order["created_at"], oid, restaurant_name,
        order["current_status"], order["total_amount"],
        order["payment_method"], summary
    ))
    print(f"  📊 [Cassandra] Đơn hàng đã sync vào order_history")


# ============================================================
# LUỒNG 1 — ĐẶT MÓN
# ============================================================

def search_by_dish(keyword):
    items = list(db.menu_items.find(
        {"name": {"$regex": keyword, "$options": "i"}, "is_available": True},
        {"restaurant_id": 1, "name": 1, "price": 1}
    ))
    if not items:
        print(f"\n  Không tìm thấy món nào chứa '{keyword}'.")
        pause()
        return []

    rest_ids = list({i["restaurant_id"] for i in items})
    rests    = list(db.restaurants.find(
        {"_id": {"$in": rest_ids}, "is_active": True}
    ).sort("avg_rating", -1))

    item_map = {}
    for i in items:
        item_map.setdefault(str(i["restaurant_id"]), []).append(i)

    for rest in rests:
        rest["_matched_items"] = item_map.get(str(rest["_id"]), [])
    return rests


def show_restaurant_list(restaurants, page=0, page_size=5):
    total = len(restaurants)
    start = page * page_size
    end   = min(start + page_size, total)
    chunk = restaurants[start:end]

    clear()
    divider("DANH SÁCH NHÀ HÀNG GỢI Ý")

    # Ranking từ Redis Sorted Set
    top_ids = r.zrevrange("ranking:restaurants", 0, 4)
    print(f"  🔴 [Redis] Ranking load từ Sorted Set\n")
    print(f"  Hiển thị {start+1}–{end} / {total} nhà hàng\n")

    for i, rest in enumerate(chunk, start + 1):
        is_top = str(rest["_id"]) in top_ids
        tag    = " 🏆 TOP" if is_top else ""
        stars  = f"⭐ {rest['avg_rating']:.1f}" if rest["avg_rating"] > 0 else "⭐ Chưa có"
        print(f"  {i}. {rest['name']}{tag} | {stars} ({rest['total_reviews']} đánh giá)")
        print(f"     📍 {rest['street']}, {rest['district']}")

        if "_matched_items" in rest:
            for item in rest["_matched_items"][:2]:
                print(f"     🍽  {item['name']} — {item['price']:,}đ")
        else:
            items = list(db.menu_items.find(
                {"restaurant_id": rest["_id"], "is_available": True, "category": "main"},
                {"name": 1, "price": 1}
            ).limit(2))
            for item in items:
                print(f"     🍽  {item['name']} — {item['price']:,}đ")
        print()

    divider()
    if start > 0:   print("  P. Trang trước")
    if end < total: print("  N. Trang tiếp")
    print("  S. Tìm theo tên món")
    print("  0. Quay lại")
    print(f"\n  Hoặc nhập số thứ tự nhà hàng để xem chi tiết")
    return chunk


def show_restaurant_detail(restaurant):
    rid_str = str(restaurant["_id"])

    while True:
        clear()
        divider(f"{restaurant['name']}")
        print(f"  📍 {restaurant['street']}, {restaurant['district']}, {restaurant['city']}")
        print(f"  🕐 {restaurant.get('open_time','07:00')} – {restaurant.get('close_time','22:00')}")
        stars = f"{restaurant['avg_rating']:.1f}" if restaurant["avg_rating"] > 0 else "Chưa có"
        print(f"  ⭐ {stars} ({restaurant['total_reviews']} đánh giá)")

        # Log activity vào Cassandra
        log_activity_cassandra("VIEW_RESTAURANT", restaurant_id=restaurant["_id"])
        log_view_restaurant(
            current_customer["_id"],
            restaurant["_id"]
        )


        # Reviews — MongoDB
        divider("ĐÁNH GIÁ GẦN ĐÂY  [MongoDB]")
        reviews = list(db.reviews.find(
            {"restaurant_id": restaurant["_id"]}
        ).sort("created_at", -1).limit(3))
        if reviews:
            for rv in reviews:
                cust = db.customers.find_one({"_id": rv["customer_id"]}, {"full_name": 1})
                name = cust["full_name"] if cust else "Ẩn danh"
                print(f"  {'★'*rv['overall_rating']}{'☆'*(5-rv['overall_rating'])} — {name}")
                if rv.get("comment"):
                    print(f"  \"{rv['comment']}\"")
                print()
        else:
            print("  Chưa có đánh giá.\n")

        # Menu — Redis cache
        divider("MENU  [Redis cache → MongoDB fallback]")
        all_items_raw = get_menu_from_cache(rid_str, restaurant["_id"])

        categories  = ["main", "drink", "dessert", "snack"]
        cat_labels  = {"main": "Món chính", "drink": "Đồ uống",
                       "dessert": "Tráng miệng", "snack": "Khai vị"}
        all_items   = []
        idx         = 1

        for cat in categories:
            items = [i for i in all_items_raw if i.get("category") == cat]
            if items:
                print(f"\n  [{cat_labels[cat].upper()}]")
                for item in items:
                    item["_idx"] = idx
                    print(f"  {idx:>2}. {item['name']:<35} {item['price']:>8,}đ")
                    if item.get("description"):
                        print(f"      {item['description'][:60]}")
                    all_items.append(item)
                    idx += 1

        divider()
        print("  Nhập số thứ tự món để thêm vào giỏ")
        print("  C. Xem giỏ hàng & đặt đơn")
        print("  0. Quay lại")

        choice = input("\n  Chọn: ").strip().upper()

        if choice == "0":
            return
        elif choice == "C":
            if not cart:
                print("\n  Giỏ hàng trống!")
                pause()
            else:
                result = checkout(restaurant)
                if result == "done":
                    return "done"
        else:
            try:
                num  = int(choice)
                item = next((i for i in all_items if i["_idx"] == num), None)
                if item:
                    try:
                        qty = int(input(f"  Số lượng '{item['name']}': "))
                        if qty <= 0: raise ValueError
                    except ValueError:
                        qty = 1

                    if cart and cart[0]["restaurant_id"] != restaurant["_id"]:
                        print("\n  ⚠️  Giỏ hàng đang có món từ nhà hàng khác!")
                        if input("  Xóa giỏ cũ và thêm món này? (y/n): ").strip().lower() == "y":
                            cart.clear()
                        else:
                            continue

                    existing = next((c for c in cart
                                     if str(c["menu_item_id"]) == str(item["_id"])), None)
                    if existing:
                        existing["quantity"]  += qty
                        existing["line_total"] = existing["unit_price"] * existing["quantity"]
                    else:
                        cart.append({
                            "menu_item_id":  ObjectId(item["_id"]) if isinstance(item["_id"], str) else item["_id"],
                            "name":          item["name"],
                            "category":      item["category"],
                            "quantity":      qty,
                            "unit_price":    item["price"],
                            "line_total":    item["price"] * qty,
                            "restaurant_id": restaurant["_id"],
                        })

                    # Log ADD_TO_CART vào Cassandra
                    log_activity_cassandra(
                        "ADD_TO_CART",
                        restaurant_id=restaurant["_id"],
                        menu_item_id=ObjectId(item["_id"]) if isinstance(item["_id"], str) else item["_id"]
                    )

                    print(f"\n  ✅ Đã thêm {qty}x {item['name']} vào giỏ!")
                    pause()
                else:
                    print("  Số không hợp lệ.")
                    pause()
            except ValueError:
                print("  Lựa chọn không hợp lệ.")
                pause()


def checkout(restaurant):
    while True:
        clear()
        divider("GIỎ HÀNG")
        print(f"  Nhà hàng: {restaurant['name']}\n")

        subtotal = sum(i["line_total"] for i in cart)
        for i, item in enumerate(cart, 1):
            print(f"  {i}. {item['name']:<35} x{item['quantity']} "
                  f"= {item['line_total']:>8,}đ")

        fee      = 15000
        discount = 0
        total    = subtotal + fee - discount

        divider()
        print(f"  Tạm tính:    {subtotal:>10,}đ")
        print(f"  Phí giao:    {fee:>10,}đ")
        print(f"  Tổng cộng:   {total:>10,}đ")
        divider()
        print("  1. Đặt đơn — CASH")
        print("  2. Đặt đơn — MOMO")
        print("  3. Đặt đơn — ZALOPAY")
        print("  X. Xóa giỏ hàng")
        print("  0. Quay lại")

        choice = input("\n  Chọn: ").strip().upper()

        if choice == "0":
            return
        elif choice == "X":
            cart.clear()
            print("\n  🗑️  Đã xóa giỏ hàng.")
            pause()
            return
        elif choice in ["1", "2", "3"]:
            pay_map = {"1": "CASH", "2": "MOMO", "3": "ZALOPAY"}
            payment = pay_map[choice]
            addr    = current_customer["addresses"][0]
            now     = datetime.now()

            order = {
                "_id":               ObjectId(),
                "customer_id":       current_customer["_id"],
                "restaurant_id":     restaurant["_id"],
                "delivery_street":   addr["street"],
                "delivery_district": addr["district"],
                "delivery_city":     addr["city"],
                "items":             [{k: v for k, v in i.items()
                                       if k != "restaurant_id"} for i in cart],
                "status_history":    [{"status": "PLACED", "timestamp": now}],
                "current_status":    "PLACED",
                "payment_method":    payment,
                "items_subtotal":    subtotal,
                "delivery_fee":      fee,
                "discount_amount":   discount,
                "total_amount":      total,
                "cancel_reason":     None,
                "created_at":        now,
                "updated_at":        now,
            }

            # Lưu vào MongoDB (source of truth)
            db.orders.insert_one(order)
            print(f"  🍃 [MongoDB] Đơn hàng đã lưu vào orders collection")

            # Cache trạng thái vào Redis
            cache_order_status(order)

            # Sync vào Cassandra order_history
            sync_order_to_cassandra(order, restaurant["name"])

            # Log ORDER_PLACED vào Cassandra activity
            log_activity_cassandra("ORDER_PLACED", restaurant_id=restaurant["_id"])
            for item in order["items"]:
                log_order_food(
                    current_customer["_id"],
                    item["menu_item_id"],
                    restaurant["_id"]
                )


            cart.clear()
            clear()
            divider("ĐẶT HÀNG THÀNH CÔNG")
            print(f"\n  ✅ Đơn hàng đã được tạo!")
            print(f"  🏪 Nhà hàng:  {restaurant['name']}")
            print(f"  💳 Thanh toán: {payment}")
            print(f"  💰 Tổng tiền:  {total:,}đ")
            print(f"  📦 Trạng thái: Chờ xác nhận")
            print(f"  🆔 Mã đơn:    ...{str(order['_id'])[-8:]}")
            pause()
            return "done"


def flow_order_food():
    restaurants  = list(db.restaurants.find({"is_active": True}).sort("avg_rating", -1))
    page         = 0
    current_list = restaurants

    while True:
        chunk  = show_restaurant_list(current_list, page=page, page_size=5)
        choice = input("\n  Chọn: ").strip().upper()

        if choice == "0":
            return
        elif choice == "N" and (page + 1) * 5 < len(current_list):
            page += 1
        elif choice == "P" and page > 0:
            page -= 1
        elif choice == "S":
            keyword = input("\n  Nhập tên món muốn tìm: ").strip()
            if keyword:
                result = search_by_dish(keyword)
                if result:
                    current_list = result
                    page = 0
                else:
                    current_list = restaurants
                    page = 0
        else:
            try:
                num       = int(choice)
                start     = page * 5
                local_idx = num - start - 1
                if 0 <= local_idx < len(chunk):
                    result = show_restaurant_detail(chunk[local_idx])
                    if result == "done":
                        return
                else:
                    print("  Số không hợp lệ.")
                    pause()
            except ValueError:
                print("  Lựa chọn không hợp lệ.")
                pause()


# ============================================================
# LUỒNG 2 — XEM ĐƠN HÀNG
# ============================================================

STATUS_LABEL = {
    "PLACED":     "⏳ Chờ xác nhận",
    "CONFIRMED":  "✅ Đã xác nhận",
    "PREPARING":  "👨‍🍳 Đang chuẩn bị",
    "PICKED_UP":  "🛵 Đã lấy hàng",
    "DELIVERING": "🚀 Đang giao",
    "COMPLETED":  "🎉 Hoàn tất",
    "CANCELLED":  "❌ Đã hủy",
}


def flow_view_orders():
    page      = 0
    page_size = 5

    while True:
        # Lịch sử từ Cassandra
        cass_rows = get_order_history_cassandra()

        # Orders đầy đủ từ MongoDB (cần để xem chi tiết + review)
        mongo_orders = list(db.orders.find(
            {"customer_id": current_customer["_id"]}
        ).sort("created_at", -1))

        # Ưu tiên hiển thị từ Cassandra, fallback MongoDB
        display_orders = mongo_orders
        total = len(display_orders)
        start = page * page_size
        end   = min(start + page_size, total)
        chunk = display_orders[start:end]

        clear()
        divider("ĐƠN HÀNG CỦA TÔI")
        print(f"  👤 {current_customer['full_name']}")
        print(f"  📊 [Cassandra] {len(cass_rows)} records trong order_history")
        print(f"  🍃 [MongoDB]   {total} đơn hàng đầy đủ\n")
        print(f"  Hiển thị {start+1}–{end} / {total} đơn\n")

        if not display_orders:
            print("  Bạn chưa có đơn hàng nào.")
            pause()
            return

        for i, o in enumerate(chunk, start + 1):
            # Thử lấy status từ Redis cache trước
            cached = r.hgetall(f"order:status:{str(o['_id'])}")
            if cached:
                status = cached.get("status", o["current_status"])
                src    = "🔴 Redis"
            else:
                status = o["current_status"]
                src    = "🍃 MongoDB"

            status_label = STATUS_LABEL.get(status, status)
            rest = db.restaurants.find_one({"_id": o["restaurant_id"]}, {"name": 1})
            rname = rest["name"] if rest else "Không rõ"

            print(f"  {i}. [{status_label}] {src}")
            print(f"     🏪 {rname} | 💰 {o['total_amount']:,}đ")
            print(f"     🕐 {o['created_at'].strftime('%d/%m/%Y %H:%M')}")
            summary = ", ".join([f"{it['name']} x{it['quantity']}"
                                  for it in o['items'][:2]])
            if len(o['items']) > 2:
                summary += f" (+{len(o['items'])-2} món)"
            print(f"     🍽  {summary}\n")

        divider()
        if start > 0:   print("  P. Trang trước")
        if end < total: print("  N. Trang tiếp")
        print("  0. Quay lại")
        print("  Nhập số thứ tự để xem chi tiết")

        choice = input("\n  Chọn: ").strip().upper()

        if choice == "0":
            return
        elif choice == "N" and end < total:
            page += 1
        elif choice == "P" and page > 0:
            page -= 1
        else:
            try:
                num       = int(choice)
                local_idx = num - start - 1
                if 0 <= local_idx < len(chunk):
                    view_order_detail(chunk[local_idx])
                else:
                    print("  Số không hợp lệ.")
                    pause()
            except ValueError:
                print("  Lựa chọn không hợp lệ.")
                pause()


def view_order_detail(order):
    while True:
        clear()
        rest  = db.restaurants.find_one({"_id": order["restaurant_id"]}, {"name": 1})
        rname = rest["name"] if rest else "Không rõ"

        # Lấy status: Redis → MongoDB
        cached_status = get_order_status_from_cache(str(order["_id"]))
        if cached_status:
            status = cached_status.get("status", order["current_status"])
        else:
            status = order["current_status"]
            print(f"  🍃 [MongoDB] Trạng thái từ MongoDB")

        status_label = STATUS_LABEL.get(status, status)

        divider("CHI TIẾT ĐƠN HÀNG")
        print(f"  🆔 Mã đơn:     ...{str(order['_id'])[-8:]}")
        print(f"  🏪 Nhà hàng:   {rname}")
        print(f"  📦 Trạng thái: {status_label}")
        print(f"  💳 Thanh toán: {order['payment_method']}")
        print(f"  🕐 Đặt lúc:    {order['created_at'].strftime('%d/%m/%Y %H:%M')}")
        print(f"  📍 Giao đến:   {order['delivery_street']}, {order['delivery_district']}")

        divider("LỊCH SỬ TRẠNG THÁI  [MongoDB]")
        for s in order["status_history"]:
            label = STATUS_LABEL.get(s["status"], s["status"])
            print(f"  {s['timestamp'].strftime('%d/%m %H:%M')}  {label}")

        divider("CÁC MÓN ĐÃ ĐẶT  [MongoDB]")
        for item in order["items"]:
            print(f"  {item['name']:<35} x{item['quantity']} "
                  f"= {item['line_total']:>8,}đ")

        divider()
        print(f"  Tạm tính:   {order['items_subtotal']:>10,}đ")
        print(f"  Phí giao:   {order['delivery_fee']:>10,}đ")
        if order['discount_amount'] > 0:
            print(f"  Giảm giá:  -{order['discount_amount']:>10,}đ")
        print(f"  Tổng cộng:  {order['total_amount']:>10,}đ")

        can_review = (order["current_status"] == "COMPLETED" and
                      not db.reviews.find_one({"order_id": order["_id"]}))

        divider()
        if can_review:
            print("  R. ⭐ Đánh giá đơn hàng này")
        print("  0. Quay lại")

        choice = input("\n  Chọn: ").strip().upper()
        if choice == "0":
            return
        elif choice == "R" and can_review:
            write_review(order)
            order = db.orders.find_one({"_id": order["_id"]})
            return


def write_review(order):
    clear()
    rest = db.restaurants.find_one({"_id": order["restaurant_id"]}, {"name": 1})
    divider("ĐÁNH GIÁ ĐƠN HÀNG")
    print(f"  Nhà hàng: {rest['name']}\n")

    def get_rating(prompt):
        while True:
            try:
                val = int(input(f"  {prompt} (1-5): "))
                if 1 <= val <= 5:
                    return val
            except ValueError:
                pass
            print("  Vui lòng nhập 1–5.")

    overall  = get_rating("⭐ Đánh giá tổng thể")
    food_q   = get_rating("🍽  Chất lượng món ăn")
    delivery = get_rating("🚀 Tốc độ giao hàng")
    comment  = input("  💬 Nhận xét (Enter để bỏ qua): ").strip()

    item_ratings = []
    print("\n  Đánh giá từng món:")
    for item in order["items"]:
        val = get_rating(f"  ★ {item['name']}")
        item_ratings.append({
            "menu_item_id": item["menu_item_id"],
            "name":         item["name"],
            "rating":       val,
        })

    now    = datetime.now()
    review = {
        "_id":                   ObjectId(),
        "order_id":              order["_id"],
        "customer_id":           current_customer["_id"],
        "restaurant_id":         order["restaurant_id"],
        "overall_rating":        overall,
        "food_quality_rating":   food_q,
        "delivery_speed_rating": delivery,
        "comment":               comment if comment else None,
        "has_photo":             False,
        "item_ratings":          item_ratings,
        "created_at":            now,
    }

    # Lưu review vào MongoDB
    db.reviews.insert_one(review)
    for item in item_ratings:
        log_review(
            current_customer["_id"],
            item["menu_item_id"],
            item["rating"]
        )


    print(f"\n  🍃 [MongoDB] Review đã lưu")

    # Cập nhật avg_rating — MongoDB atomic
    agg = list(db.reviews.aggregate([
        {"$match": {"restaurant_id": order["restaurant_id"]}},
        {"$group": {"_id": None,
                    "avg": {"$avg": "$overall_rating"},
                    "cnt": {"$sum": 1}}}
    ]))
    if agg:
        db.restaurants.update_one(
            {"_id": order["restaurant_id"]},
            {"$set": {"avg_rating":    round(agg[0]["avg"], 2),
                      "total_reviews": agg[0]["cnt"]}}
        )
        # Cập nhật Redis ranking
        r.zadd("ranking:restaurants",
               {str(order["restaurant_id"]): round(agg[0]["avg"], 2)})
        print(f"  🔴 [Redis] Ranking nhà hàng đã cập nhật")

    clear()
    divider("CẢM ƠN ĐÁNH GIÁ CỦA BẠN!")
    print(f"\n  ⭐ Tổng thể:   {'★'*overall}{'☆'*(5-overall)}")
    print(f"  🍽  Món ăn:    {'★'*food_q}{'☆'*(5-food_q)}")
    print(f"  🚀 Giao hàng: {'★'*delivery}{'☆'*(5-delivery)}")
    if comment:
        print(f"  💬 \"{comment}\"")
    print(f"\n  Đánh giá đã lưu vào MongoDB!")
    pause()


# ============================================================
# MAIN
# ============================================================

def main():
    pick_customer()
    while True:
        clear()
        divider(f"FOOD DELIVERY — Xin chào, {current_customer['full_name']}!")
        print()
        print("  1. 🍜  Muốn ăn gì? (Tìm & đặt món)")
        print("  2. 📦  Đơn hàng của tôi")
        print("  0. 🚪  Thoát")
        print()
        choice = input("  Chọn chức năng: ").strip()

        if choice == "1":
            flow_order_food()
        elif choice == "2":
            flow_view_orders()
        elif choice == "0":
            clear()
            print("\n  Tạm biệt! 👋\n")
            break
        else:
            print("  Lựa chọn không hợp lệ.")
            pause()


if __name__ == "__main__":
    main()