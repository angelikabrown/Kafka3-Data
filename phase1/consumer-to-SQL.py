from kafka import KafkaConsumer, TopicPartition
from json import loads

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        #app = Flask(__name__)
        #app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///transactions.db'
        #app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        #app.config['SECRET_KEY'] = 'angie'  

        #initilaze the database with the app, connecting them
        #db.init_app(app)

        #Create the database if it doesn't exist
        #if not path.exists('transactions.db'):
        #with app.app_context():
        #   db.create_all()
        #   print("Database created!")
        #   print(db)        
        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
