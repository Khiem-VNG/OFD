# customer.py
import os
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import uuid

# ============================================================
# KẾT NỐI
# ============================================================
db = MongoClient("mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin")["food_delivery"]

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

def pause():
    input("\n  [Nhấn Enter để tiếp tục]")

# ============================================================
# LUỒNG 1 — ĐẶT MÓN
# ============================================================

def search_by_dish(keyword):
    """Tìm nhà hàng có món chứa keyword"""
    items = list(db.menu_items.find({"name": {"$regex": keyword, "$options": "i"}, "is_available": True},{"restaurant_id": 1, "name": 1, "price": 1}))

    if not items:
        print(f"\n  Không tìm thấy món nào chứa '{keyword}'.")
        pause()
        return []

    # Nhóm theo nhà hàng
    rest_ids = list({i["restaurant_id"] for i in items})
    rests = list(db.restaurants.find(
        {"_id": {"$in": rest_ids}, "is_active": True}
    ).sort("avg_rating", -1))

    # Gắn món tìm được vào từng nhà hàng
    item_map = {}
    for i in items:
        rid = str(i["restaurant_id"])
        item_map.setdefault(rid, []).append(i)

    result = []
    for r in rests:
        r["_matched_items"] = item_map.get(str(r["_id"]), [])
        result.append(r)
    return result


def show_restaurant_list(restaurants, page=0, page_size=5):
    """Hiển thị danh sách nhà hàng phân trang"""
    total = len(restaurants)
    start = page * page_size
    end   = min(start + page_size, total)
    chunk = restaurants[start:end]

    clear()
    divider("DANH SÁCH NHÀ HÀNG GỢI Ý")
    print(f"  Hiển thị {start+1}–{end} / {total} nhà hàng\n")

    for i, r in enumerate(chunk, start + 1):
        stars = f"⭐ {r['avg_rating']:.1f}" if r['avg_rating'] > 0 else "⭐ Chưa có"
        print(f"  {i}. {r['name']} | {stars} ({r['total_reviews']} đánh giá)")
        print(f"     📍 {r['street']}, {r['district']}")

        # Hiển thị vài món nổi bật
        if "_matched_items" in r:
            for item in r["_matched_items"][:2]:
                print(f"     🍽  {item['name']} — {item['price']:,}đ")
        else:
            items = list(db.menu_items.find({"restaurant_id": r["_id"], "is_available": True, "category": "main"},{"name": 1, "price": 1}).limit(2))

            for item in items:
                print(f"     🍽  {item['name']} — {item['price']:,}đ")
        print()

    divider()
    options = []
    if start > 0:
        options.append("P. Trang trước")
    if end < total:
        options.append("N. Trang tiếp")
    options.append("S. Tìm theo tên món")
    options.append("0. Quay lại")

    for o in options:
        print(f"  {o}")
    print(f"\n  Hoặc nhập số thứ tự nhà hàng để xem chi tiết")

    return chunk


def show_restaurant_detail(restaurant):
    """Hiển thị chi tiết nhà hàng: info + review + menu"""
    while True:
        clear()
        r = restaurant
        divider(f"{r['name']}")
        print(f"  📍 {r['street']}, {r['district']}, {r['city']}")
        print(f"  🕐 {r.get('open_time','07:00')} – {r.get('close_time','22:00')}")
        stars = f"{r['avg_rating']:.1f}" if r['avg_rating'] > 0 else "Chưa có"
        print(f"  ⭐ {stars} ({r['total_reviews']} đánh giá)")

        # Reviews gần nhất
        divider("ĐÁNH GIÁ GẦN ĐÂY")
        reviews = list(db.reviews.find({"restaurant_id": r["_id"]}).sort("created_at", -1).limit(3))

        if reviews:
            for rv in reviews:
                cust = db.customers.find_one({"_id": rv["customer_id"]}, {"full_name": 1})
                name = cust["full_name"] if cust else "Ẩn danh"
                print(f"  ★{'★'*rv['overall_rating']}{'☆'*(5-rv['overall_rating'])} — {name}")
                if rv.get("comment"):
                    print(f"  \"{rv['comment']}\"")
                print()
        else:
            print("  Chưa có đánh giá.\n")

        # Menu theo category
        divider("MENU")
        categories = ["main", "drink", "dessert", "snack"]
        cat_labels  = {"main": "Món chính", "drink": "Đồ uống","dessert": "Tráng miệng", "snack": "Khai vị"}
        all_items = []
        idx = 1
        for cat in categories:
            items = list(db.menu_items.find({"restaurant_id": r["_id"], "category": cat, "is_available": True},{"name": 1, "price": 1, "description": 1, "category": 1}))

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
        print("  Nhập số thứ tự món để thêm vào giỏ hàng")
        print("  C. Xem giỏ hàng & đặt đơn")
        print("  0. Quay lại danh sách nhà hàng")

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
                num = int(choice)
                item = next((i for i in all_items if i["_idx"] == num), None)
                if item:
                    # Hỏi số lượng
                    try:
                        qty = int(input(f"  Số lượng '{item['name']}': "))
                        if qty <= 0:
                            raise ValueError
                    except ValueError:
                        qty = 1

                    # Kiểm tra giỏ hàng có đang từ nhà hàng khác không
                    if cart and cart[0]["restaurant_id"] != restaurant["_id"]:
                        print("\n  ⚠️  Giỏ hàng đang có món từ nhà hàng khác!")
                        print("  Xóa giỏ hàng cũ và thêm món này? (y/n)")
                        if input("  ").strip().lower() == "y":
                            cart.clear()
                        else:
                            continue

                    # Thêm vào giỏ
                    existing = next((c for c in cart if c["menu_item_id"] == item["_id"]), None)
                    if existing:
                        existing["quantity"] += qty
                        existing["line_total"] = existing["unit_price"] * existing["quantity"]
                    else:
                        cart.append({
                            "menu_item_id":  item["_id"],
                            "name":          item["name"],
                            "category":      item["category"],
                            "quantity":      qty,
                            "unit_price":    item["price"],
                            "line_total":    item["price"] * qty,
                            "restaurant_id": restaurant["_id"],
                        })
                    print(f"\n  ✅ Đã thêm {qty}x {item['name']} vào giỏ hàng!")
                    pause()
                else:
                    print("  Số không hợp lệ.")
                    pause()
            except ValueError:
                print("  Lựa chọn không hợp lệ.")
                pause()


def checkout(restaurant):
    """Xem giỏ hàng và tạo đơn"""
    while True:
        clear()
        divider("GIỎ HÀNG")
        print(f"  Nhà hàng: {restaurant['name']}\n")

        subtotal = 0
        for i, item in enumerate(cart, 1):
            print(f"  {i}. {item['name']:<35} x{item['quantity']} "f"= {item['line_total']:>8,}đ")
            subtotal += item["line_total"]

        fee      = 15000
        discount = 0
        total    = subtotal + fee - discount

        divider()
        print(f"  Tạm tính:     {subtotal:>10,}đ")
        print(f"  Phí giao:     {fee:>10,}đ")
        print(f"  Tổng cộng:    {total:>10,}đ")

        divider()
        print("  1. Xác nhận đặt đơn (COD)")
        print("  2. Xác nhận đặt đơn (MOMO)")
        print("  3. Xác nhận đặt đơn (ZALOPAY)")
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

            addr = current_customer["addresses"][0]
            now  = datetime.now()

            order = {
                "_id":               ObjectId(),
                "customer_id":       current_customer["_id"],
                "restaurant_id":     restaurant["_id"],
                "delivery_street":   addr["street"],
                "delivery_district": addr["district"],
                "delivery_city":     addr["city"],
                "items":             [{k: v for k, v in i.items() if k != "restaurant_id"} for i in cart],
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

            db.orders.insert_one(order)
            cart.clear()

            clear()
            divider("ĐẶT HÀNG THÀNH CÔNG")
            print(f"\n  ✅ Đơn hàng đã được tạo!")
            print(f"  🏪 Nhà hàng: {restaurant['name']}")
            print(f"  💳 Thanh toán: {payment}")
            print(f"  💰 Tổng tiền: {total:,}đ")
            print(f"  📦 Trạng thái: Chờ xác nhận")
            print(f"  🆔 Mã đơn: {str(order['_id'])[-8:]}")
            pause()
            return "done"


def flow_order_food():
    """Luồng 1: Tìm món & đặt hàng"""
    restaurants = list(db.restaurants.find(
        {"is_active": True}
    ).sort("avg_rating", -1))

    page = 0
    current_list = restaurants

    while True:
        chunk = show_restaurant_list(current_list, page=page, page_size=5)
        choice = input("\n  Chọn: ").strip().upper()

        if choice == "0":
            return
        elif choice == "N":
            total = len(current_list)
            if (page + 1) * 5 < total:
                page += 1
        elif choice == "P":
            if page > 0:
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
                num = int(choice)
                start = page * 5
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
    """Luồng 2: Xem danh sách đơn hàng"""
    page = 0
    page_size = 5

    while True:
        orders = list(db.orders.find({"customer_id": current_customer["_id"]}).sort("created_at", -1))
        total = len(orders)
        start = page * page_size
        end   = min(start + page_size, total)
        chunk = orders[start:end]

        clear()
        divider("ĐƠN HÀNG CỦA TÔI")
        print(f"  Khách hàng: {current_customer['full_name']}")
        print(f"  Hiển thị {start+1}–{end} / {total} đơn\n")

        if not orders:
            print("  Bạn chưa có đơn hàng nào.")
            pause()
            return

        for i, o in enumerate(chunk, start + 1):
            rest = db.restaurants.find_one({"_id": o["restaurant_id"]}, {"name": 1})
            rname = rest["name"] if rest else "Không rõ"
            status_label = STATUS_LABEL.get(o["current_status"], o["current_status"])
            print(f"  {i}. [{status_label}]")
            print(f"     🏪 {rname} | 💰 {o['total_amount']:,}đ")
            print(f"     🕐 {o['created_at'].strftime('%d/%m/%Y %H:%M')}")
            # Tóm tắt món
            summary = ", ".join([f"{it['name']} x{it['quantity']}" for it in o['items'][:2]])
            if len(o['items']) > 2:
                summary += f" (+{len(o['items'])-2} món)"
            print(f"     🍽  {summary}\n")

        divider()
        if start > 0:
            print("  P. Trang trước")
        if end < total:
            print("  N. Trang tiếp")
        print("  0. Quay lại")
        print("  Nhập số thứ tự để xem chi tiết đơn hàng")

        choice = input("\n  Chọn: ").strip().upper()

        if choice == "0":
            return
        elif choice == "N" and end < total:
            page += 1
        elif choice == "P" and page > 0:
            page -= 1
        else:
            try:
                num = int(choice)
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
    """Xem chi tiết 1 đơn hàng"""
    while True:
        clear()
        rest = db.restaurants.find_one({"_id": order["restaurant_id"]}, {"name": 1})
        rname = rest["name"] if rest else "Không rõ"
        status_label = STATUS_LABEL.get(order["current_status"], order["current_status"])

        divider(f"CHI TIẾT ĐƠN HÀNG")
        print(f"  🆔 Mã đơn:    ...{str(order['_id'])[-8:]}")
        print(f"  🏪 Nhà hàng:  {rname}")
        print(f"  📦 Trạng thái: {status_label}")
        print(f"  💳 Thanh toán: {order['payment_method']}")
        print(f"  🕐 Đặt lúc:   {order['created_at'].strftime('%d/%m/%Y %H:%M')}")
        print(f"  📍 Giao đến:  {order['delivery_street']}, {order['delivery_district']}")

        divider("LỊCH SỬ TRẠNG THÁI")
        for s in order["status_history"]:
            label = STATUS_LABEL.get(s["status"], s["status"])
            print(f"  {s['timestamp'].strftime('%d/%m %H:%M')}  {label}")

        divider("CÁC MÓN ĐÃ ĐẶT")
        for item in order["items"]:
            print(f"  {item['name']:<35} x{item['quantity']} "
                  f"= {item['line_total']:>8,}đ")

        divider()
        print(f"  Tạm tính:   {order['items_subtotal']:>10,}đ")
        print(f"  Phí giao:   {order['delivery_fee']:>10,}đ")
        if order['discount_amount'] > 0:
            print(f"  Giảm giá:  -{order['discount_amount']:>10,}đ")
        print(f"  Tổng cộng:  {order['total_amount']:>10,}đ")

        # Kiểm tra có thể đánh giá không
        can_review = (order["current_status"] == "COMPLETED" and not db.reviews.find_one({"order_id": order["_id"]}))

        divider()
        if can_review:
            print("  R. Đánh giá đơn hàng này")
        print("  0. Quay lại")

        choice = input("\n  Chọn: ").strip().upper()

        if choice == "0":
            return
        elif choice == "R" and can_review:
            write_review(order)
            # Reload order sau khi review
            order = db.orders.find_one({"_id": order["_id"]})
            return


def write_review(order):
    """Viết đánh giá cho đơn hàng"""
    clear()
    divider("ĐÁNH GIÁ ĐƠN HÀNG")
    rest = db.restaurants.find_one({"_id": order["restaurant_id"]}, {"name": 1})
    print(f"  Nhà hàng: {rest['name']}\n")

    def get_rating(prompt):
        while True:
            try:
                r = int(input(f"  {prompt} (1-5): "))
                if 1 <= r <= 5:
                    return r
            except ValueError:
                pass
            print("  Vui lòng nhập số từ 1 đến 5.")

    overall  = get_rating("⭐ Đánh giá tổng thể")
    food_q   = get_rating("🍽  Chất lượng món ăn")
    delivery = get_rating("🚀 Tốc độ giao hàng")
    comment  = input("  💬 Nhận xét (Enter để bỏ qua): ").strip()

    # Rating từng món
    item_ratings = []
    print("\n  Đánh giá từng món:")
    for item in order["items"]:
        r = get_rating(f"  ★ {item['name']}")
        item_ratings.append({
            "menu_item_id": item["menu_item_id"],
            "name":         item["name"],
            "rating":       r,
        })

    now = datetime.now()
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
    db.reviews.insert_one(review)

    # Cập nhật avg_rating nhà hàng
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

    clear()
    divider("CẢM ƠN ĐÁNH GIÁ CỦA BẠN!")
    print(f"\n  ⭐ Tổng thể:    {'★' * overall}{'☆' * (5-overall)}")
    print(f"  🍽  Món ăn:     {'★' * food_q}{'☆' * (5-food_q)}")
    print(f"  🚀 Giao hàng:  {'★' * delivery}{'☆' * (5-delivery)}")
    if comment:
        print(f"  💬 \"{comment}\"")
    print(f"\n  Đánh giá đã được lưu!")
    pause()


# ============================================================
# MAIN MENU
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