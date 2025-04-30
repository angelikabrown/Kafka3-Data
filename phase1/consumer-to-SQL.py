from kafka import KafkaConsumer, TopicPartition
from json import loads
from flask import Flask
from models import db, Transaction
import numpy as np



# run ./start-kafka.sh in Kafka3-Data directory
# next, run the producer in phase1 directory
# then run this consumer in the same directory
# This is assuming your database is running and transaction table created  

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted

        self.ledger = {}

        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        self.deposits = []
        self.withdrawals = []

        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!

        # add a way to connect to your database here.
        self.app = Flask(__name__)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://angel_b:gore1984@localhost/customer'
        #initialize the database
        db.init_app(self.app)
        with self.app.app_context():
            #create the database
    
            db.create_all()
    def update_summary(self, transaction):
        #update the summary of the transaction
        if transaction['type'] == 'dep':
            self.deposits.append(transaction['amt'])
        elif transaction['type'] == 'wth':
            self.withdrawals.append(transaction['amt'])
        self.print_summary()

    def print_summary(self):
        if self.deposits:
            mean_deposit = np.mean(self.deposits)
            std_deposit = np.std(self.deposits)
        else:
            mean_deposit = 0
            std_deposit = 0
        
        if self.withdrawals:
            mean_withdrawal = np.mean(self.withdrawals)
            std_withdrawal = np.std(self.withdrawals)
        else:
            mean_withdrawal = 0
            std_withdrawal = 0
        
        print("\n ----- Numerical Summary -----")
        print("Mean Deposit: ", mean_deposit)
        print("Standard Deviation Deposit: ", std_deposit)
        print("Mean Withdrawal: ", mean_withdrawal)
        print("Standard Deviation Withdrawal: ", std_withdrawal)
        print("-----------------------------")


    def handleMessages(self):
        with self.app.app_context():
            for message in self.consumer:
                message = message.value
                print('{} received'.format(message))
                transaction = Transaction(
                    custid=message['custid'],
                    type=message['type'],
                    date=message['date'],
                    amt=message['amt']

                )
                db.session.add(transaction)
                db.session.commit()
                self.update_summary(message)
                print("Transaction added to database")
        
        

    

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
    
