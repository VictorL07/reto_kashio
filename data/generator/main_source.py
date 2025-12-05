"""
Mock Data Generator for Digital Services Inc. - Data Engineering Challenge
Generates sample data for three sources:
1. App Events (Streaming) - JSON
2. Transactions (Batch) - CSV
3. Users (Dimensional) - CSV/SQLite

Author: Data Engineering Team
Date: 2024
"""

import json
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockDataGenerator:
    """Generator for mock data across multiple sources"""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize the data generator
        
        Args:
            output_dir: Directory where data files will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Configuration
        self.num_users = 100
        self.num_sessions = 500
        self.num_events = 2000
        self.num_transactions = 300
        
        # Reference data
        self.countries = ['PE', 'US', 'MX', 'CO', 'CL', 'AR', 'BR', 'EC']
        self.device_types = ['iOS', 'Android', 'Web', 'Tablet']
        self.event_types = ['page_view', 'click', 'scroll', 'login', 'logout', 
                           'add_to_cart', 'checkout', 'search', 'signup']
        self.currencies = ['USD', 'PEN', 'MXN', 'COP', 'CLP', 'ARS', 'BRL']
        
        # Generated data storage
        self.users = []
        self.sessions = []
        self.events = []
        self.transactions = []
        
        logger.info(f"Initialized MockDataGenerator with output directory: {self.output_dir}")
    
    def generate_users(self) -> List[Dict]:
        """
        Generate dimensional user data
        
        Returns:
            List of user dictionaries
        """
        logger.info(f"Generating {self.num_users} users...")
        
        base_date = datetime.now() - timedelta(days=365)
        
        for user_id in range(1, self.num_users + 1):
            signup_date = base_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            user = {
                'user_id': f'USR_{user_id:05d}',
                'signup_date': signup_date.strftime('%Y-%m-%d %H:%M:%S'),
                'device_type': random.choice(self.device_types),
                'country': random.choice(self.countries)
            }
            self.users.append(user)
        
        logger.info(f"Generated {len(self.users)} users")
        return self.users
    
    def generate_sessions(self) -> List[Dict]:
        """
        Generate session identifiers linked to users
        
        Returns:
            List of session dictionaries
        """
        logger.info(f"Generating {self.num_sessions} sessions...")
        
        if not self.users:
            raise ValueError("Users must be generated before sessions")
        
        base_date = datetime.now() - timedelta(days=90)
        
        for session_id in range(1, self.num_sessions + 1):
            user = random.choice(self.users)
            session_start = base_date + timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            session = {
                'session_id': f'SES_{session_id:06d}',
                'user_id': user['user_id'],
                'session_start': session_start
            }
            self.sessions.append(session)
        
        logger.info(f"Generated {len(self.sessions)} sessions")
        return self.sessions
    
    def generate_events(self) -> List[Dict]:
        """
        Generate streaming app events (JSON format)
        
        Returns:
            List of event dictionaries
        """
        logger.info(f"Generating {self.num_events} events...")
        
        if not self.sessions:
            raise ValueError("Sessions must be generated before events")
        
        for event_id in range(1, self.num_events + 1):
            session = random.choice(self.sessions)
            
            # Events happen within session timeframe
            event_timestamp = session['session_start'] + timedelta(
                minutes=random.randint(0, 30),
                seconds=random.randint(0, 59)
            )
            
            event_type = random.choice(self.event_types)
            
            # Generate event-specific details
            event_details = self._generate_event_details(event_type)
            
            # Simulate some late-arriving data (5% of events)
            if random.random() < 0.05:
                event_timestamp = event_timestamp - timedelta(hours=random.randint(2, 48))
            
            event = {
                'event_id': f'EVT_{event_id:08d}',
                'session_id': session['session_id'],
                'user_id': session['user_id'],
                'event_type': event_type,
                'event_timestamp': event_timestamp.isoformat(),
                'event_details': event_details
            }
            self.events.append(event)
        
        logger.info(f"Generated {len(self.events)} events")
        return self.events
    
    def _generate_event_details(self, event_type: str) -> Dict:
        """
        Generate event-specific details based on event type
        
        Args:
            event_type: Type of event
            
        Returns:
            Dictionary with event details
        """
        details = {}
        
        if event_type == 'page_view':
            details = {
                'page_url': f'/page/{random.randint(1, 50)}',
                'referrer': random.choice(['google', 'facebook', 'direct', 'email'])
            }
        elif event_type == 'search':
            details = {
                'search_query': random.choice(['laptop', 'phone', 'tablet', 'headphones', 'camera']),
                'results_count': random.randint(0, 100)
            }
        elif event_type == 'add_to_cart':
            details = {
                'product_id': f'PROD_{random.randint(1, 1000):04d}',
                'quantity': random.randint(1, 5),
                'price': round(random.uniform(10, 1000), 2)
            }
        elif event_type == 'click':
            details = {
                'element_id': f'btn_{random.randint(1, 20)}',
                'element_type': random.choice(['button', 'link', 'image'])
            }
        else:
            details = {'action': event_type}
        
        return details
    
    def generate_transactions(self) -> List[Dict]:
        """
        Generate batch transaction data (CSV format)
        
        Returns:
            List of transaction dictionaries
        """
        logger.info(f"Generating {self.num_transactions} transactions...")
        
        if not self.sessions:
            raise ValueError("Sessions must be generated before transactions")
        
        # Only some sessions have transactions
        sessions_with_transactions = random.sample(
            self.sessions, 
            min(self.num_transactions, len(self.sessions))
        )
        
        for txn_id, session in enumerate(sessions_with_transactions, 1):
            # Transaction happens within session
            transaction_timestamp = session['session_start'] + timedelta(
                minutes=random.randint(5, 30),
                seconds=random.randint(0, 59)
            )
            
            # Simulate some duplicate transactions (2% duplication rate)
            is_duplicate = random.random() < 0.02
            
            transaction = {
                'transaction_id': f'TXN_{txn_id:07d}',
                'session_id': session['session_id'],
                'user_id': session['user_id'],
                'amount': round(random.uniform(10, 5000), 2),
                'currency': random.choice(self.currencies),
                'transaction_timestamp': transaction_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            }
            self.transactions.append(transaction)
            
            # Add duplicate if flagged
            if is_duplicate:
                self.transactions.append(transaction.copy())
        
        logger.info(f"Generated {len(self.transactions)} transactions (including potential duplicates)")
        return self.transactions
    
    def save_users_csv(self):
        """Save users data to CSV file"""
        output_file = self.output_dir / "users.csv"
        logger.info(f"Saving users to {output_file}")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if self.users:
                writer = csv.DictWriter(f, fieldnames=self.users[0].keys())
                writer.writeheader()
                writer.writerows(self.users)
        
        logger.info(f"Saved {len(self.users)} users to {output_file}")
    
    def save_events_json(self):
        """Save events data to JSON file (one event per line - JSONL format)"""
        output_file = self.output_dir / "events.jsonl"
        logger.info(f"Saving events to {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for event in self.events:
                f.write(json.dumps(event) + '\n')
        
        logger.info(f"Saved {len(self.events)} events to {output_file}")
    
    def save_transactions_csv(self):
        """Save transactions data to CSV file"""
        output_file = self.output_dir / "transactions.csv"
        logger.info(f"Saving transactions to {output_file}")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if self.transactions:
                writer = csv.DictWriter(f, fieldnames=self.transactions[0].keys())
                writer.writeheader()
                writer.writerows(self.transactions)
        
        logger.info(f"Saved {len(self.transactions)} transactions to {output_file}")
    
    def generate_all(self):
        """Generate all data sources and save to files"""
        logger.info("Starting full data generation process...")
        
        try:
            # Generate data in order (dependencies)
            self.generate_users()
            self.generate_sessions()
            self.generate_events()
            self.generate_transactions()
            
            # Save to files
            self.save_users_csv()
            self.save_events_json()
            self.save_transactions_csv()
            
            logger.info("Data generation completed successfully!")
            self._print_summary()
            
        except Exception as e:
            logger.error(f"Error during data generation: {str(e)}")
            raise
    
    def _print_summary(self):
        """Print summary statistics of generated data"""
        print("\n" + "="*50)
        print("DATA GENERATION SUMMARY")
        print("="*50)
        print(f"Output Directory: {self.output_dir}")
        print(f"\nGenerated Files:")
        print(f"  - users.csv: {len(self.users)} records")
        print(f"  - events.jsonl: {len(self.events)} records")
        print(f"  - transactions.csv: {len(self.transactions)} records")
        print(f"\nData Characteristics:")
        print(f"  - Date Range: Last 90 days")
        print(f"  - Late Arrivals: ~5% of events")
        print(f"  - Duplicates: ~2% of transactions")
        print(f"  - Countries: {', '.join(self.countries)}")
        print("="*50 + "\n")


def main():
    """Main execution function"""
    try:
        # Initialize generator
        generator = MockDataGenerator(output_dir="data_test")
        
        # Generate all data
        generator.generate_all()
        
    except Exception as e:
        logger.error(f"Failed to generate data: {str(e)}")
        raise


if __name__ == "__main__":
    main()