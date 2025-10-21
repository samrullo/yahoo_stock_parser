import os
import pathlib
from dotenv import load_dotenv

load_dotenv()


class Config:
    base_folder = pathlib.Path(os.getenv("BASE_FOLDER"))
    stock_db_path = base_folder / "datasets" / "stock.db"
    db_uri = f"sqlite:///{stock_db_path}"
    local_mysql_uri = "mysql://root:@localhost/stocks?charset=utf8"
    droplet_mysql_uri = "mysql://samrullo:18Rirezu@68.183.81.44/stocks?charset=utf8"
