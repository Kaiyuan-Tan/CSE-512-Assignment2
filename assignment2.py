from pymongo import MongoClient
import json

# Updated database and collection names
DATABASE_NAME = 'ecommerce'
COLLECTION_NAME = 'orders'


def insert_mock_data():
    """Inserts the generated mock data in JSON file into the MongoDB."""
    with open('MOCK_DATA.json', 'r') as file:
        data = json.load(file)
    insert_data = db[COLLECTION_NAME].insert_many(data)
    print("Data inserted successfully")
    print(insert_data.inserted_ids)


def find_order_totals():
    """Finds the total number of orders and the number of orders per state, sorted by count in ascending order."""
    total_orders = db[COLLECTION_NAME].count_documents({})
    print("Total number of orders: ", total_orders)

    pipeline = [
        {"$group": {"_id": "$state","count": {"$sum": 1}}},
        {"$sort": {"count": 1}}
    ]
    result = db[COLLECTION_NAME].aggregate(pipeline)
    print("Number of orders per state: ")
    for state in result:
        print("State: ", state["_id"], ", Count: ", state["count"])
    print("==========================================")


def find_product_frequencies():
    """Finds the products and their frequencies sorted by frequency in descending order."""
    pipeline = [
        {"$group": {"_id": "$product_id","count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    result = db[COLLECTION_NAME].aggregate(pipeline)
    print("Product Frequencies: ")
    for product in result:
        print("Product ID: ", product["_id"], ", Frequency: ", product["count"])
    print("==========================================")


def ca_highvalue_orders():
    """Counts and finds the orders in California where the order amount exceeds $1,000."""
    pipeline_1 = {"state": "California","total_price": {"$gt": 1000}}
    pipeline_2 = [
        {"$match":{"state": "California","total_price": {"$gt": 1000}}},
    ]

    count = db[COLLECTION_NAME].count_documents(pipeline_1)
    print("Total high-value orders in California: ", count)
    if count == 0:
        print("High-value orders details: None")
    else:
        result = db[COLLECTION_NAME].aggregate(pipeline_2)
        print("High-value orders details: ")
        for order in result:
            print(f"Order ID: {order['order_id']}, Customer ID: {order['customer_id']}, Quantity, {order['quantity']}, Unit price: {order['unit_price']}, Order Date: {order['order_date']}, State: {order['state']}, Total Pirce: {order['total_price']}, Preminm Customer: {order['premium_customer']}, City: {order['city']}")
    print("==========================================")

 

def top_states_highvalue():
    """Finds the top ten states with the most orders where the order amount exceeds $500."""
    pipeline = [
        {"$match":{"total_price": {"$gt": 500}}},
        {"$group": {"_id": "$state","count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]

    print("Top 10 States with High-Value Orders (>$500): ")
    rank = 1
    result = db[COLLECTION_NAME].aggregate(pipeline)
    for order in result:
        print(f"Rank {rank}: State: {order['_id']}, Order Count: {order['count']}")
        rank = rank + 1
    print("==========================================")

  

def find_customer_premium():
    """Counts and finds the customers who have placed premium orders (order amount exceeds $2,000) in Texas."""
    pipeline_1 = {"state": "Texas", "total_price":{"$gt":2000}}
    pipeline_2 = [
        {"$match":{"state": "Texas", "total_price":{"$gt":2000}}}
        ]
    count = db[COLLECTION_NAME].count_documents(pipeline_1)
    result = db[COLLECTION_NAME].aggregate(pipeline_2)

    print("Total premium customer in Texas: ", count)
    print("Premium customer details: ")
    for order in result:
        print(f"Order ID: {order['order_id']}, Customer ID: {order['customer_id']}, Quantity, {order['quantity']}, Unit price: {order['unit_price']}, Order Date: {order['order_date']}, State: {order['state']}, Total Pirce: {order['total_price']}, Preminm Customer: {order['premium_customer']}, City: {order['city']}")
    print("==========================================")


def find_orders_by_date(order_date):
    """Counts and finds the orders placed in New York City on a specific date."""
    pipeline_1 = [
        {"$match": {"city": "New York City", "order_date": order_date}}
    ]
    pipeline_2 = {'city': 'New York City','order_date': order_date}
    result = db[COLLECTION_NAME].aggregate(pipeline_1)
    count = db[COLLECTION_NAME].count_documents(pipeline_2)

    print(f"Total orders placed in New York City on {order_date}: {count}")
    if count == 0:
        print("Order Detail: None")
    else:
        print("Order Detail: ")
        for order in result:
            print(f"Order ID: {order['order_id']}, Customer ID: {order['customer_id']}, Quantity, {order['quantity']}, Unit price: {order['unit_price']}, Order Date: {order['order_date']}, State: {order['state']}, Total Pirce: {order['total_price']}, Preminm Customer: {order['premium_customer']}, City: {order['city']}")



           

if __name__ == '__main__':
    # Connect to the ecommerce database and perform operations
    db = MongoClient()[DATABASE_NAME]

    insert_mock_data()
    find_order_totals()
    find_product_frequencies()
    ca_highvalue_orders()
    top_states_highvalue()
    find_customer_premium()
    
    # Example date in the format 'MM/DD/YYYY'
    specific_date = '1/9/2021' #You may change it to the date you want.
    # Call the function to find orders by date in NYC
    find_orders_by_date(specific_date)
   
