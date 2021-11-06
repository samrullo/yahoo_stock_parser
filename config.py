import os


class Config:
    stock_db_path = os.path.join(r"C:\Users\amrul\PycharmProjects\stocks", "datasets", "stock.db")
    db_uri = f"sqlite:///{stock_db_path}"
    local_mysql_uri = "mysql://root:@localhost/stocks?charset=utf8"
    droplet_mysql_uri = 'mysql://samrullo:18Rirezu@68.183.81.44/stocks?charset=utf8'
