import random
import time
from datetime import datetime

def generate_user_id():
    return f"user_{random.randint(1, 1000)}"

def generate_action_and_url():
    actions = {
        "login": "/login",
        "logout": "/logout",
        "view_page": random.choice(["/home", "/about", "/products"]),
        "click_button": random.choice(["/submit", "/click"]),
        "submit_form": "/submit",
        "add_to_cart": "/add-to-cart",
        "checkout": "/checkout",
        "view_cart": "/view-cart",
        "apply_promo_code": "/apply-promo-code",
        "initiate_return": "/initiate-return",
        "track_order": "/track-order",
        "view_promotions": "/view-promotions",
        "open_support_ticket": "/open-support-ticket"
    }
    action = random.choice(list(actions.keys()))
    url = actions[action]
    return action, url

def generate_response_time():
    return round(random.uniform(0.1, 3.0), 2)

def generate_status_and_detail():
    if random.random() < 0.3:
        errors = [
            ("500 Internal Server Error", "Server encountered an unexpected condition."),
            ("404 Not Found", "The requested resource was not found."),
            ("403 Forbidden", "Access to the resource is forbidden."),
            ("401 Unauthorized", "Authentication is required and has failed.")
        ]
        error_code, error_message = random.choice(errors)
        return error_code, f"{error_code} | Detail: {error_message}"
    else:
        successes = [
            ("200 OK", "Request succeeded."),
            ("201 Created", "The request succeeded, and a new resource was created."),
            ("204 No Content", "The server successfully processed the request.")
        ]
        success_code, success_message = random.choice(successes)
        return success_code , f"{success_code} | Detail: {success_message}"

def generate_request_id():
    return f"req_{random.randint(10000, 99999)}"

def generate_ip_address():
    return f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"

def generate_url():
    urls = ["/home", "/about", "/contact", "/products", "/login"]
    return random.choice(urls)

def generate_http_method(action):
    if action in ["login", "submit_form", "add_to_cart", "checkout", "apply_promo_code", "initiate_return", "open_support_ticket"]:
        return "POST"
    elif action in ["logout", "view_page", "view_cart", "track_order", "view_promotions"]:
        return "GET"
    elif action == "click_button":
        return "PUT"
    return "GET"

def generate_session_id():
    return f"session_{random.randint(1000, 9999)}"

def generate_log_level(status_code):
    if "200" in status_code or "201" in status_code or "204" in status_code:
        return "INFO"
    elif "404" in status_code or "403" in status_code:
        return "WARN"
    elif "500" in status_code or "401" in status_code:
        return "ERROR"
    return "DEBUG"

def generate_referrer_url():
    referrers = ["https://www.google.com", "https://www.bing.com", "Direct Entry"]
    return random.choice(referrers)

def generate_user_agent():
    user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                   "Mozilla/5.0 (iPhone; CPU iPhone OS 13_3 like Mac OS X)"]
    return random.choice(user_agents)

def generate_latency_breakdown():
    db_query_time = round(random.uniform(0.01, 0.5), 2)
    server_processing_time = round(random.uniform(0.01, 0.5), 2)
    network_latency = round(random.uniform(0.01, 0.5), 2)
    total_response_time = db_query_time + server_processing_time + network_latency
    return db_query_time, server_processing_time, network_latency, total_response_time

def generate_app_specific_data():
    product_id = f"prod_{random.randint(100, 999)}"
    cart_size = random.randint(1, 10)
    checkout_status = random.choice(["pending", "completed", "failed"])
    return product_id, cart_size, checkout_status

def generate_auth_details():
    token = f"token_{random.randint(10000, 99999)}"
    auth_method = random.choice(["OAuth", "JWT", "Basic"])
    auth_level = random.choice(["user", "admin"])
    return token, auth_method, auth_level

def generate_correlation_id():
    return f"corr_{random.randint(10000, 99999)}"

def introduce_anomalies():
    anomaly_chance = 0.2
    if random.random() < anomaly_chance:
        return {
            "spike_response_time": round(random.uniform(5, 10), 2),
            "unusual_action": "unusual_action",
            "high_cart_size": random.randint(100, 1000),
            "system_error": "500 Internal Server Error | Detail: Unexpected system error"
        }
    return {}

def generate_network_info():
    server_ip = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
    port_number = random.randint(1024, 65535)
    protocol = random.choice(["HTTP", "HTTPS"])
    return server_ip, port_number, protocol

def generate_log_entry():
    anomalies = introduce_anomalies()

    timestamp = datetime.now().isoformat()
    user_id = generate_user_id()
    action, url = generate_action_and_url()
    response_time = generate_response_time()
    status_code, status_and_detail = generate_status_and_detail()
    request_id = generate_request_id()
    ip_address = generate_ip_address()
    user_agent = generate_user_agent()
    http_method = generate_http_method(action)
    log_level = generate_log_level(status_code)
    referrer_url = generate_referrer_url()
    session_id = generate_session_id()
    db_query_time, server_processing_time, network_latency, response_time = generate_latency_breakdown()
    product_id, cart_size, checkout_status = generate_app_specific_data()
    token, auth_method, auth_level = generate_auth_details()
    correlation_id = generate_correlation_id()
    server_ip, port_number, protocol = generate_network_info()
    response_time = anomalies.get("spike_response_time", response_time)
    action = anomalies.get("unusual_action", action)
    cart_size = anomalies.get("high_cart_size", cart_size)

    log_entry = f"{timestamp} | {log_level} | database query time : {db_query_time} | server processing time : {server_processing_time} | network latency : {network_latency} | Response Time: {response_time}s | {request_id} | {session_id} | {user_id} | {action} | {http_method} | {url} | Referrer: {referrer_url} | IP: {ip_address} | Agent: {user_agent} | Product ID: {product_id} | Cart Size: {cart_size} | Checkout Status: {checkout_status} | Token: {token} | Auth Method: {auth_method} | Auth Level: {auth_level} | Correlation ID: {correlation_id} | Server IP: {server_ip} | Port: {port_number} | Protocol: {protocol} | {status_and_detail}"
    return log_entry

while True:
    print(generate_log_entry())
    time.sleep(random.uniform(0.2, 1.0))
