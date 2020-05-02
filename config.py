import os


class Config:
    stock_db_path = os.path.join(r"C:\Users\amrul\PycharmProjects\stocks", "datasets", "stock.db")
    db_uri = f"sqlite:///{stock_db_path}"
