import json
import boto3
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from typing import List, Dict

# SQLAlchemy Base
Base = declarative_base()


# Restaurant Entity (Python version matching Java entity)
class Restaurant(Base):
    __tablename__ = "restaurant"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    address = Column(String(500), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    thumbnail = Column(String(500))


class S3ToRDSLoader:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_region: str,
        s3_bucket: str,
        s3_key: str,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
    ):

        # S3 client setup
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        # Database setup
        self.engine = create_engine(
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
            echo=True,  # Set to False in production
        )
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def read_from_s3(self) -> List[Dict]:
        """Read JSON data from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
            content = response["Body"].read().decode("utf-8")
            data = json.loads(content)
            print(f"Successfully read {len(data)} records from S3")
            return data
        except Exception as e:
            print(f"Error reading from S3: {e}")
            raise

    def save_to_rds(self, restaurants_data: List[Dict]) -> int:
        """Save restaurant data to RDS"""
        saved_count = 0

        try:
            for restaurant_data in restaurants_data:
                # Check if restaurant already exists by name and address
                existing = (
                    self.session.query(Restaurant)
                    .filter_by(
                        name=restaurant_data["name"], address=restaurant_data["address"]
                    )
                    .first()
                )

                if existing:
                    print(
                        f"Restaurant already exists: {restaurant_data['name']} at {restaurant_data['address']}"
                    )
                    continue
                print(restaurant_data)
                # Create new restaurant
                restaurant = Restaurant(
                    name=restaurant_data["name"],
                    address=(
                        restaurant_data["address"]
                        if restaurant_data["address"] is not None
                        else ""
                    ),
                    latitude=(
                        restaurant_data["latitude"]
                        if restaurant_data["latitude"] is not None
                        else 0
                    ),
                    longitude=(
                        restaurant_data["longitude"]
                        if restaurant_data["longitude"] is not None
                        else 0
                    ),
                    thumbnail=restaurant_data.get(
                        "thumbnail", None
                    ),  # thumbnail is optional
                )

                self.session.add(restaurant)
                saved_count += 1
                print(f"Added restaurant: {restaurant.name}")

            # Commit all changes
            self.session.commit()
            print(f"Successfully saved {saved_count} new restaurants to RDS")
            return saved_count

        except Exception as e:
            self.session.rollback()
            print(f"Error saving to RDS: {e}")
            raise
        finally:
            self.session.close()

    def load_data(self):
        """Main method to load data from S3 to RDS"""
        print("Starting data load from S3 to RDS...")

        # Read from S3
        restaurants_data = self.read_from_s3()

        # Save to RDS
        saved_count = self.save_to_rds(restaurants_data)

        print(f"Data load complete. Total records saved: {saved_count}")


# Usage example
if __name__ == "__main__":
    # Configuration - replace with your actual values
    config = {
        # AWS Configuration
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "your-access-key"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-key"),
        "aws_region": os.getenv("AWS_REGION", "ap-northeast-2"),
        "s3_bucket": os.getenv("S3_BUCKET_NAME", "your-bucket-name"),
        "s3_key": os.getenv("S3_KEY", "path/to/restaurants.json"),
        # RDS Configuration
        "db_host": os.getenv("DB_HOST", "your-rds-endpoint.amazonaws.com"),
        "db_port": int(os.getenv("DB_PORT", "3306")),
        "db_name": os.getenv("DB_NAME", "your-database-name"),
        "db_user": os.getenv("DB_USER", "your-db-username"),
        "db_password": os.getenv("DB_PASSWORD", "your-db-password"),
    }

    # Create loader and execute
    loader = S3ToRDSLoader(**config)
    loader.load_data()

# Alternative: Using environment variables from .env file
# from dotenv import load_dotenv
# load_dotenv()
#
# config = {
#     'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
#     'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
#     'aws_region': os.getenv('AWS_REGION'),
#     's3_bucket': os.getenv('S3_BUCKET'),
#     's3_key': os.getenv('S3_KEY'),
#     'db_host': os.getenv('DB_HOST'),
#     'db_port': int(os.getenv('DB_PORT')),
#     'db_name': os.getenv('DB_NAME'),
#     'db_user': os.getenv('DB_USER'),
#     'db_password': os.getenv('DB_PASSWORD')
# }
