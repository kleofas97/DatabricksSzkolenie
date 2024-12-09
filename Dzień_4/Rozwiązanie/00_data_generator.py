# Databricks notebook source
import pandas as pd
import random
from datetime import datetime
import numpy as np

# Konfiguracja
start_date = datetime(2010, 1, 1)
end_date = datetime(2024, 1, 1)
num_users = 100  # Liczba użytkowników
num_products = 20  # Liczba produktów

def random_date_by_month(start, end):
    # Generate a random year and month within the range
    start_year, end_year = start.year, end.year
    year = random.randint(start_year, end_year)

    # Determine the month range for the selected year
    start_month = start.month if year == start_year else 1
    end_month = end.month if year == end_year else 12
    month = random.randint(start_month, end_month)

    # Determine the last day of the selected month
    last_day = (datetime(year, month + 1, 1) - datetime(year, month, 1)).days if month != 12 else 31
    day = random.randint(1, last_day)

    # Return the random date
    return datetime(year, month, day)

# Inicjalizacja Faker
names = [
    "John", "Emily", "Michael", "Sarah", "David", "Jessica", "Daniel", "Ashley",
    "Matthew", "Amanda", "Christopher", "Jennifer", "Andrew", "Elizabeth",
    "Joshua", "Megan", "James", "Lauren", "Joseph", "Rachel", "Ryan", "Kimberly",
    "Nicholas", "Heather", "Anthony", "Stephanie", "Jonathan", "Nicole",
    "Brian", "Courtney", "William", "Amber", "Justin", "Hannah", "Alexander",
    "Brittany", "Tyler", "Melissa", "Zachary", "Kayla", "Kevin", "Victoria",
    "Brandon", "Samantha", "Adam", "Taylor", "Nathan", "Caitlin", "Eric", "Chelsea"
]

last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts"
]

products = [
    {"Product": "Laptop", "Price": 999.99},
    {"Product": "Smartphone", "Price": 699.99},
    {"Product": "Tablet", "Price": 399.99},
    {"Product": "Smartwatch", "Price": 199.99},
    {"Product": "Headphones", "Price": 149.99},
    {"Product": "Bluetooth Speaker", "Price": 89.99},
    {"Product": "Keyboard", "Price": 49.99},
    {"Product": "Mouse", "Price": 29.99},
    {"Product": "Monitor", "Price": 249.99},
    {"Product": "Printer", "Price": 129.99},
    {"Product": "External Hard Drive", "Price": 79.99},
    {"Product": "Flash Drive", "Price": 19.99},
    {"Product": "Webcam", "Price": 39.99},
    {"Product": "Router", "Price": 59.99},
    {"Product": "Smart Light Bulb", "Price": 24.99},
    {"Product": "Drone", "Price": 499.99},
    {"Product": "Action Camera", "Price": 299.99},
    {"Product": "Electric Scooter", "Price": 899.99},
    {"Product": "Gaming Console", "Price": 499.99},
    {"Product": "VR Headset", "Price": 349.99}
]


# Lista krajów
countries = [
    "USA", "Canada", "UK", "Germany", "France", "Italy", "Spain",
    "Australia", "India", "Brazil", "China", "Japan", "South Korea",
    "Mexico", "Netherlands", "Russia", "South Africa", "Sweden"
]

# Generowanie danych użytkowników
user_data = []
for user_id in range(len(names)*len(last_names)):
    name = random.choice(names)
    surname = random.choice(last_names)
    email = f"{name.lower()}.{surname.lower()}@example.com"
    country = random.choice(countries)
    user_data.append([user_id, name, surname, email, country])

# Generowanie danych produktów
product_data = []
for product_id in range(len(products)):
    product_name = random.choice(products)
    price = product_name["Price"]  # Cena w przedziale 10-500
    product_data.append((product_id, product_name["Product"], price))

# Generowanie danych zakupów
purchase_data = []
for user in user_data:
    user_id = user[0]
    srednia = 35
    odchylenie_standardowe = 10  # Możesz dostosować do swoich potrzeb

    # Generowanie liczby z rozkładu normalnego
    num_purchases = int(np.random.normal(loc=srednia, scale=odchylenie_standardowe))

    # Sprawdzanie, czy liczba mieści się w zakresie 1-50
    num_purchases = max(1, min(70, num_purchases))
    for _ in range(num_purchases):
        product = random.choice(product_data)
        purchase_date = random_date_by_month(start_date, end_date)
        purchase_data.append([user_id, product[0], purchase_date, product[2]])

# Tworzenie DataFrames
users_df = pd.DataFrame(user_data, columns=["ID", "First_Name", "Last_Name", "Email", "Country"])
products_df = pd.DataFrame(product_data, columns=["ID", "Product_Name", "Price"])
purchases_df = pd.DataFrame(purchase_data, columns=["User_ID", "Product_ID", "Purchase_Date", "Price"])

# Zapisywanie do plików CSV
users_df.to_csv("users.csv", index=False)
purchases_df.to_csv("purchases.csv", index=False)

split_size = len(purchases_df) // 2
df1 = purchases_df.iloc[:split_size]
df2 = purchases_df.iloc[split_size:2*split_size]

df1.to_csv("purchases.csv", index=False)
df2.to_csv("purchases2.csv", index=False)
products_df.to_csv("products.csv", index=False)
