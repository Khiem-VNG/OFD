from pymongo import MongoClient
from datetime import datetime, timezone
from bson import ObjectId
import json
import uuid
import redis
from cassandra.cluster import Cluster


# ──────────────────────────────────────────
#  DB Connection
# ──────────────────────────────────────────
db  = MongoClient("mongodb://admin:admin123@localhost:27017")["food_delivery"]
r   = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
cass = Cluster(["localhost"], port=9042).connect("food_delivery")

VALID_STATUSES   = ["PLACED", "CONFIRMED", "PREPARING", "PICKED_UP", "DELIVERING", "COMPLETED", "CANCELLED"]
VALID_CATEGORIES = ["main", "drink", "dessert", "snack"]

# TTL constants (seconds)
TTL_RESTAURANTS = 120   # Danh sách nhà hàng  — ít thay đổi
TTL_MENU        = 180   # Menu nhà hàng        — ít thay đổi
TTL_ORDERS      = 30    # Đơn hàng             — thay đổi thường xuyên
TTL_STATS       = 300   # Thống kê doanh thu   — tính toán nặng, cache dài


# ──────────────────────────────────────────
#  Cassandra Prepared Statements
# ──────────────────────────────────────────
stmt_log_activity = cass.prepare("""
    INSERT INTO customer_activity
    (customer_id, timestamp, event_id, event_type,
     restaurant_id, menu_item_id, search_keyword)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")

# Dùng customer_id hệ thống (sentinel) để log event từ phía nhà hàng
# (vì Cassandra partition theo customer_id)
SYSTEM_CUSTOMER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


# ──────────────────────────────────────────
#  Helpers — UI
# ──────────────────────────────────────────
def clear():
    print("\n" + "=" * 55)

def header(title):
    print("=" * 55)
    print(f"  {title}")
    print("=" * 55)

def prompt(label, required=True):
    while True:
        val = input(f"  {label}: ").strip()
        if val or not required:
            return val
        print("  ⚠  Trường này không được để trống.")

def choose(label, options):
    print(f"\n  {label}")
    for i, opt in enumerate(options, 1):
        print(f"    {i}. {opt}")
    print("    0. Quay lại")
    while True:
        raw = input("  Chọn: ").strip()
        if raw == "0":
            return None
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        print("  ⚠  Lựa chọn không hợp lệ, thử lại.")

def confirm(msg):
    return input(f"  {msg} (y/n): ").strip().lower() == "y"


# ──────────────────────────────────────────
#  Helpers — Cache
# ──────────────────────────────────────────
def _rid_str(restaurant):
    return str(restaurant["_id"])

def _rid_cass(restaurant):
    return uuid.uuid5(uuid.NAMESPACE_OID, _rid_str(restaurant))

def _invalidate_menu_cache(restaurant):
    """Xoá tất cả cache liên quan đến menu của nhà hàng."""
    rid = _rid_str(restaurant)
    r.delete(f"menu:full:{rid}")
    r.delete(f"menu:{rid}")

def _invalidate_order_cache(restaurant):
    """Xoá cache đơn hàng (tất cả + theo từng status)."""
    rid = _rid_str(restaurant)
    r.delete(f"orders:all:{rid}")
    for status in VALID_STATUSES:
        r.delete(f"orders:{rid}:{status}")

def _invalidate_stats_cache(restaurant):
    """Xoá cache thống kê doanh thu."""
    rid = _rid_str(restaurant)
    r.delete(f"stats:income:{rid}")
    # Xoá tất cả cache doanh thu theo khoảng ngày (prefix scan)
    for key in r.scan_iter(f"stats:income:{rid}:*"):
        r.delete(key)


# ──────────────────────────────────────────
#  1. Danh sách & chọn nhà hàng
#  Tối ưu: Redis cache TTL 120s
# ──────────────────────────────────────────
def list_and_select_restaurant():
    clear()
    header("DANH SÁCH NHÀ HÀNG")

    # [OPTIMIZED] Đọc từ Redis trước, fallback MongoDB
    cache_key = "restaurants:list"
    cached = r.get(cache_key)
    if cached:
        restaurants = json.loads(cached)
        # Convert _id string back to ObjectId cho các thao tác MongoDB
        for res in restaurants:
            if isinstance(res.get("_id"), str):
                res["_id"] = ObjectId(res["_id"])
    else:
        restaurants = list(db.restaurants.find())
        r.set(cache_key, json.dumps(restaurants, default=str), ex=TTL_RESTAURANTS)

    if not restaurants:
        print("  Không có nhà hàng nào.")
        return None

    for i, res in enumerate(restaurants, 1):
        status = "✅ Hoạt động" if res.get("is_active") else "🔴 Tạm nghỉ"
        rating = f"⭐ {res.get('avg_rating', 'N/A')}" if res.get("avg_rating") else ""
        print(f"  {i:>2}. {res['name']:<30} {status}  {rating}")

    print("   0. Thoát")
    while True:
        raw = input("\n  Chọn nhà hàng: ").strip()
        if raw == "0":
            return None
        if raw.isdigit() and 1 <= int(raw) <= len(restaurants):
            return restaurants[int(raw) - 1]
        print("  ⚠  Lựa chọn không hợp lệ.")


# ──────────────────────────────────────────
#  2. Menu chức năng
# ──────────────────────────────────────────
def restaurant_menu(restaurant):
    while True:
        clear()
        header(f"NHÀ HÀNG: {restaurant['name'].upper()}")
        print("  1. Quản lý menu")
        print("  2. Quản lý đơn hàng")
        print("  3. Thống kê")
        print("  0. Quay lại danh sách")

        choice = input("\n  Chọn chức năng: ").strip()
        if choice == "1":
            menu_manage(restaurant)
        elif choice == "2":
            order_menu(restaurant)
        elif choice == "3":
            stats_menu(restaurant)
        elif choice == "0":
            break
        else:
            print("  ⚠  Lựa chọn không hợp lệ.")


# ──────────────────────────────────────────
#  3. Quản lý menu
# ──────────────────────────────────────────
def menu_manage(restaurant):
    while True:
        clear()
        header("QUẢN LÝ MENU")
        print(f"  Nhà hàng: {restaurant['name']}\n")
        print("  1. Xem tất cả món trong menu")
        print("  2. Thêm món ăn mới")
        print("  0. Quay lại")

        choice = input("\n  Chọn: ").strip()
        if choice == "1":
            view_menu(restaurant)
        elif choice == "2":
            add_dish(restaurant)
        elif choice == "0":
            break
        else:
            print("  ⚠  Lựa chọn không hợp lệ.")


def view_menu(restaurant):
    """
    [OPTIMIZED] Redis cache TTL 180s.
    Menu ít thay đổi → cache dài hạn, invalidate khi thêm/sửa/xóa món.
    """
    clear()
    header("TẤT CẢ MÓN TRONG MENU")
    print(f"  Nhà hàng: {restaurant['name']}\n")

    rid = _rid_str(restaurant)
    cache_key = f"menu:full:{rid}"

    cached = r.get(cache_key)
    if cached:
        items = json.loads(cached)
        # Reattach ObjectId cho restaurant_id nếu cần
        for item in items:
            if isinstance(item.get("restaurant_id"), str):
                item["restaurant_id"] = ObjectId(item["restaurant_id"])
    else:
        items = list(db.menu_items.find({"restaurant_id": restaurant["_id"]}).sort("category", 1))
        r.set(cache_key, json.dumps(items, default=str), ex=TTL_MENU)

    if not items:
        print("  (Chưa có món nào trong menu)")
        input("\n  Nhấn Enter để quay lại...")
        return

    grouped = {}
    for item in items:
        cat = item.get("category", "other")
        grouped.setdefault(cat, []).append(item)

    category_labels = {"main": "Món chính", "drink": "Đồ uống", "dessert": "Tráng miệng", "snack": "Ăn vặt"}

    for cat, dishes in grouped.items():
        label = category_labels.get(cat, cat.upper())
        print(f"\n  ── {label} ({'─' * (40 - len(label))})")
        print(f"  {'Tên món':<28} {'Giá':>10}  {'Trạng thái'}")
        print("  " + "-" * 52)
        for d in dishes:
            avail = "✅ Có sẵn" if d.get("is_available") else "🔴 Tạm hết"
            name  = d.get("name", "?")
            price = d.get("price", 0)
            print(f"  {name:<28} {price:>8,}₫  {avail}")
            if d.get("description"):
                print(f"  {'':28}   ↳ {d['description']}")

    print(f"\n  Tổng cộng: {len(items)} món")
    input("\n  Nhấn Enter để quay lại...")


def add_dish(restaurant):
    """
    [OPTIMIZED]
    - Ghi vào MongoDB (source of truth)
    - Invalidate Redis menu cache ngay lập tức
    - Log event vào Cassandra (audit trail)
    """
    while True:
        clear()
        header("THÊM MÓN ĂN MỚI")
        print(f"  Nhà hàng: {restaurant['name']}\n")

        name = prompt("Tên món")

        category = choose("Danh mục", VALID_CATEGORIES)
        if category is None:
            break

        while True:
            price_raw = prompt("Giá (VNĐ, số nguyên)")
            if price_raw.isdigit() and int(price_raw) >= 0:
                price = int(price_raw)
                break
            print("  ⚠  Giá phải là số nguyên không âm.")

        description = prompt("Mô tả (có thể bỏ trống)", required=False)

        avail_input = input("  Có sẵn? (y/n, mặc định y): ").strip().lower()
        is_available = avail_input != "n"

        print(f"""
  ┌─ Xác nhận thông tin ───────────────────┐
  │  Tên      : {name}
  │  Danh mục : {category}
  │  Giá      : {price:,}₫
  │  Mô tả    : {description or '(trống)'}
  │  Trạng thái: {'Có sẵn' if is_available else 'Tạm hết'}
  └────────────────────────────────────────┘""")

        if not confirm("Xác nhận thêm món?"):
            print("  ↩  Đã huỷ.")
            input("  Nhấn Enter để tiếp tục...")
            continue

        now = datetime.now(timezone.utc)
        new_dish = {
            "restaurant_id": restaurant["_id"],
            "name":          name,
            "category":      category,
            "price":         price,
            "is_available":  is_available,
            "description":   description,
            "updated_at":    now,
        }

        # 1. Ghi MongoDB
        result = db.menu_items.insert_one(new_dish)

        if result.inserted_id:
            # 2. [OPTIMIZED] Invalidate Redis menu cache
            _invalidate_menu_cache(restaurant)

            # 3. [OPTIMIZED] Log event vào Cassandra
            try:
                menu_item_cass_id = uuid.uuid5(uuid.NAMESPACE_OID, str(result.inserted_id))
                cass.execute(stmt_log_activity, (
                    SYSTEM_CUSTOMER_ID,
                    now,
                    uuid.uuid4(),
                    "MENU_ITEM_ADDED",
                    _rid_cass(restaurant),
                    menu_item_cass_id,
                    None,
                ))
            except Exception as e:
                # Cassandra log thất bại không nên làm hỏng luồng chính
                print(f"  ⚠  Cassandra log thất bại (không ảnh hưởng): {e}")

            print(f"\n  ✅ Đã thêm món '{name}' với ID: {result.inserted_id}")
        else:
            print("  ❌ Thêm món thất bại.")

        if not confirm("\n  Thêm món khác?"):
            break


# ──────────────────────────────────────────
#  4. Quản lý đơn hàng
# ──────────────────────────────────────────
def order_menu(restaurant):
    while True:
        clear()
        header("QUẢN LÝ ĐƠN HÀNG")
        print(f"  Nhà hàng: {restaurant['name']}\n")
        print("  1. Xem tất cả đơn hàng")
        print("  2. Xem đơn theo trạng thái")
        print("  3. Cập nhật trạng thái đơn hàng")
        print("  0. Quay lại")

        choice = input("\n  Chọn: ").strip()
        if choice == "1":
            view_all_orders(restaurant)
        elif choice == "2":
            view_orders_by_status(restaurant)
        elif choice == "3":
            update_order_status(restaurant)
        elif choice == "0":
            break
        else:
            print("  ⚠  Lựa chọn không hợp lệ.")


def _print_orders(orders):
    if not orders:
        print("  (Không có đơn hàng nào)")
        return
    for o in orders:
        items_str = ", ".join(
            f"{it.get('name', '?')} x{it.get('quantity', it.get('qty', 1))}"
            for it in o.get("items", [])
        )
        print(f"""
  ┌────────────────────────────────────────
  │  Order ID  : {o['_id']}
  │  Khách     : {o.get('customer_id', 'N/A')}
  │  Món       : {items_str}
  │  Tổng      : {o.get('total_amount', 0):,}₫
  │  Trạng thái: {o.get('current_status', 'N/A')}
  └────────────────────────────────────────""")


def view_all_orders(restaurant):
    """
    [OPTIMIZED] Redis cache TTL 30s.
    Đơn hàng thay đổi thường xuyên → TTL ngắn để cân bằng tốc độ & độ tươi.
    """
    clear()
    header("TẤT CẢ ĐƠN HÀNG")
    print(f"  Nhà hàng: {restaurant['name']}\n")

    rid = _rid_str(restaurant)
    cache_key = f"orders:all:{rid}"

    cached = r.get(cache_key)
    if cached:
        orders = json.loads(cached)
        for o in orders:
            if isinstance(o.get("_id"), str):
                o["_id"] = ObjectId(o["_id"])
    else:
        orders = list(db.orders.find({"restaurant_id": restaurant["_id"]}))
        r.set(cache_key, json.dumps(orders, default=str), ex=TTL_ORDERS)

    print(f"  Tổng số đơn: {len(orders)}")
    _print_orders(orders)
    input("\n  Nhấn Enter để quay lại...")


def view_orders_by_status(restaurant):
    """
    [OPTIMIZED] Redis cache per-status TTL 30s.
    Compound query (restaurant_id + status) → cache riêng từng status.
    """
    clear()
    header("XEM ĐƠN THEO TRẠNG THÁI")
    status = choose("Chọn trạng thái", VALID_STATUSES)
    if status is None:
        return

    rid = _rid_str(restaurant)
    cache_key = f"orders:{rid}:{status}"

    cached = r.get(cache_key)
    if cached:
        orders = json.loads(cached)
        for o in orders:
            if isinstance(o.get("_id"), str):
                o["_id"] = ObjectId(o["_id"])
    else:
        orders = list(db.orders.find({
            "restaurant_id": restaurant["_id"],
            "current_status": status
        }))
        r.set(cache_key, json.dumps(orders, default=str), ex=TTL_ORDERS)

    clear()
    header(f"ĐƠN HÀNG — {status}")
    print(f"  Nhà hàng: {restaurant['name']}\n")
    print(f"  Số đơn: {len(orders)}")
    _print_orders(orders)
    input("\n  Nhấn Enter để quay lại...")


def update_order_status(restaurant):
    """
    [OPTIMIZED]
    - Cập nhật MongoDB (source of truth)
    - Sync Redis hash ngay lập tức (customer thấy status mới tức thì)
    - Invalidate order list cache
    - Log status change event vào Cassandra (audit trail đầy đủ)
    """
    clear()
    header("CẬP NHẬT TRẠNG THÁI ĐƠN HÀNG")
    print(f"  Nhà hàng: {restaurant['name']}\n")

    orders = list(db.orders.find({"restaurant_id": restaurant["_id"]}).sort("_id", -1))
    if not orders:
        print("  (Không có đơn hàng nào)")
        input("\n  Nhấn Enter để quay lại...")
        return

    print(f"  {'#':>3}  {'Order ID':<26} {'Trạng thái':<14} {'Tổng tiền':>12}")
    print("  " + "─" * 60)
    for i, o in enumerate(orders, 1):
        items_str = ", ".join(it.get("name", "?") for it in o.get("items", []))
        print(f"  {i:>3}. {str(o['_id']):<26} {o.get('current_status', '?'):<14} {o.get('total_amount', 0):>10,}₫")
        print(f"  {'':>5}  ↳ {items_str}")
    print("  " + "─" * 60)
    print("    0. Quay lại")

    while True:
        raw = input("\n  Chọn số thứ tự đơn cần cập nhật: ").strip()
        if raw == "0":
            return
        if raw.isdigit() and 1 <= int(raw) <= len(orders):
            order = orders[int(raw) - 1]
            break
        print("  ⚠  Lựa chọn không hợp lệ.")

    print(f"\n  Đơn #{raw} — {order['_id']}")
    print(f"  Trạng thái hiện tại: {order['current_status']}")

    new_status = choose("Chọn trạng thái mới", VALID_STATUSES)
    if new_status is None:
        return

    now     = datetime.now(timezone.utc)
    oid_str = str(order["_id"])

    # 1. Ghi MongoDB
    result = db.orders.update_one(
        {"_id": order["_id"]},
        {"$set": {"current_status": new_status, "updated_at": now}}
    )

    if result.modified_count > 0:
        # 2. [OPTIMIZED] Sync Redis hash để customer thấy status mới ngay
        r.hset(f"order:status:{oid_str}", mapping={
            "status":     new_status,
            "updated_at": now.isoformat(),
        })
        r.expire(f"order:status:{oid_str}", 7200)

        # 3. [OPTIMIZED] Invalidate order list cache
        _invalidate_order_cache(restaurant)
        # Invalidate stats vì COMPLETED ảnh hưởng doanh thu
        if new_status == "COMPLETED" or order.get("current_status") == "COMPLETED":
            _invalidate_stats_cache(restaurant)

        # 4. [OPTIMIZED] Log status change vào Cassandra
        try:
            cass.execute(stmt_log_activity, (
                SYSTEM_CUSTOMER_ID,
                now,
                uuid.uuid4(),
                f"ORDER_{new_status}",
                _rid_cass(restaurant),
                None,
                None,
            ))
        except Exception as e:
            print(f"  ⚠  Cassandra log thất bại (không ảnh hưởng): {e}")

        print(f"\n  ✅ Đã cập nhật đơn #{raw} → {new_status}")
    else:
        print("  ⚠  Không có thay đổi nào được thực hiện.")

    input("  Nhấn Enter để quay lại...")


# ──────────────────────────────────────────
#  5. Thống kê
# ──────────────────────────────────────────
def stats_menu(restaurant):
    while True:
        clear()
        header("THỐNG KÊ")
        print(f"  Nhà hàng: {restaurant['name']}\n")
        print("  1. Tổng doanh thu (toàn thời gian)")
        print("  2. Doanh thu theo khoảng thời gian")
        print("  0. Quay lại")

        choice = input("\n  Chọn: ").strip()
        if choice == "1":
            show_total_income(restaurant)
        elif choice == "2":
            show_income_by_interval(restaurant)
        elif choice == "0":
            break
        else:
            print("  ⚠  Lựa chọn không hợp lệ.")


def show_total_income(restaurant):
    """
    [OPTIMIZED] MongoDB aggregate → cache Redis TTL 300s.
    Aggregate nặng, kết quả ít thay đổi → cache dài để tránh scan collection.
    """
    clear()
    header("TỔNG DOANH THU")
    print(f"  Nhà hàng: {restaurant['name']}\n")

    rid = _rid_str(restaurant)
    cache_key = f"stats:income:{rid}"

    cached = r.get(cache_key)
    if cached:
        data  = json.loads(cached)
        total = data["total"]
        count = data["count"]
    else:
        pipeline = [
            {"$match": {"restaurant_id": restaurant["_id"], "current_status": "COMPLETED"}},
            {"$group": {"_id": None, "total": {"$sum": "$total_amount"}, "count": {"$sum": 1}}}
        ]
        agg = list(db.orders.aggregate(pipeline))
        if agg:
            total = agg[0]["total"]
            count = agg[0]["count"]
        else:
            total, count = 0, 0

        # Cache kết quả aggregate
        r.set(cache_key, json.dumps({"total": total, "count": count}), ex=TTL_STATS)

    print(f"  Đơn hoàn thành : {count} đơn")
    print(f"  Tổng doanh thu : {total:,}₫")
    input("\n  Nhấn Enter để quay lại...")


def parse_date(label):
    while True:
        raw = input(f"  {label} (dd/mm/yyyy): ").strip()
        try:
            return datetime.strptime(raw, "%d/%m/%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            print("  ⚠  Định dạng ngày không hợp lệ, thử lại.")


def show_income_by_interval(restaurant):
    """
    [OPTIMIZED] MongoDB aggregate theo date range → cache Redis TTL 300s.
    Cache key bao gồm start/end date → tránh stale data khi đổi khoảng thời gian.
    Data lịch sử không thay đổi → TTL dài an toàn.
    """
    clear()
    header("DOANH THU THEO KHOẢNG THỜI GIAN")
    print(f"  Nhà hàng: {restaurant['name']}\n")

    start_date = parse_date("Từ ngày")
    end_date   = parse_date("Đến ngày")

    if start_date > end_date:
        print("  ❌ Ngày bắt đầu phải trước ngày kết thúc.")
        input("  Nhấn Enter để quay lại...")
        return

    rid       = _rid_str(restaurant)
    start_key = start_date.strftime("%Y%m%d")
    end_key   = end_date.strftime("%Y%m%d")
    cache_key = f"stats:income:{rid}:{start_key}:{end_key}"

    cached = r.get(cache_key)
    if cached:
        results = json.loads(cached)
    else:
        pipeline = [
            {"$match": {
                "restaurant_id": restaurant["_id"],
                "current_status": "COMPLETED",
                "created_at": {"$gte": start_date, "$lte": end_date}
            }},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                "daily_income": {"$sum": "$total_amount"},
                "count":        {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        results = list(db.orders.aggregate(pipeline))
        r.set(cache_key, json.dumps(results, default=str), ex=TTL_STATS)

    print(f"\n  Từ {start_date.strftime('%d/%m/%Y')} → {end_date.strftime('%d/%m/%Y')}\n")
    print(f"  {'Ngày':<14} {'Số đơn':>8} {'Doanh thu':>14}")
    print("  " + "-" * 38)

    total = 0
    for row in results:
        total += row["daily_income"]
        print(f"  {row['_id']:<14} {row['count']:>8} {row['daily_income']:>12,}₫")

    if not results:
        print("  (Không có dữ liệu trong khoảng này)")

    print("  " + "─" * 38)
    print(f"  {'TỔNG':<14} {'':>8} {total:>12,}₫")
    input("\n  Nhấn Enter để quay lại...")


# ──────────────────────────────────────────
#  Main
# ──────────────────────────────────────────
def main():
    while True:
        restaurant = list_and_select_restaurant()
        if restaurant is None:
            clear()
            print("\n  👋 Tạm biệt!\n")
            break
        restaurant_menu(restaurant)


if __name__ == "__main__":
    main()