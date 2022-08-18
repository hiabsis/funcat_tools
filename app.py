from flask import Flask
import main.data_util as data_util
from funcat import *
app = Flask(__name__)


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


@app.route('/update_data')
def update_data():  # put application's code here
    """
    更新数据
    :return:
    """
    data_util.update_date()
    return 'update_data'


if __name__ == '__main__':
    app.run()
