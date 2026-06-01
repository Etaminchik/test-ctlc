import sqlite3
import pickle
import sys

class AddressProcessing:
    def __init__(self, dadata_db: str):
        self.dadata_db = dadata_db
        self.NEW_DB = 'structured_addresses_from_dadata.sqlite'

    def create_new_database(self):
        with sqlite3.connect(self.NEW_DB) as conn:
            cursor = conn.cursor()
            cursor.execute('''
        CREATE TABLE IF NOT EXISTS addresses (
        unstructured_address TEXT,
        postal_code TEXT CHECK(LENGTH(postal_code) <= 6),
        country TEXT CHECK(LENGTH(country) <= 128),
        region_type TEXT CHECK(LENGTH(region_type) <= 128),
        region TEXT CHECK(LENGTH(region) <= 128),
        city_district_type TEXT CHECK(LENGTH(city_district_type) <= 128),
        city_district TEXT CHECK(LENGTH(city_district) <= 128),
        city_type TEXT CHECK(LENGTH(city_type) <= 128),
        city TEXT CHECK(LENGTH(city) <= 128),
        street_type TEXT CHECK(LENGTH(street_type) <= 128),
        street TEXT CHECK(LENGTH(street) <= 128),
        house TEXT CHECK(LENGTH(house) <= 32),
        block TEXT CHECK(LENGTH(block) <= 32),
        flat_type TEXT CHECK(LENGTH(flat_type) <= 128),
        flat TEXT CHECK(LENGTH(flat) <= 32)
        )
        ''')
            conn.commit()#save


    def insert_address(self,unstructured_address,address):
        postal_code         = address['postal_code']
        country             = address['country']
        region_type         = address['region_type']
        region              = address['region']
        city_district_type  = address['city_district_type']
        city_district       = address['city_district']
        city_type           = address.get('city_type') or address.get('settlement_type')
        city                = address.get('city') or address.get('settlement')
        street_type         = address['street_type']
        street              = address['street']
        house               = address['house']
        block               = address['block']
        flat_type           = address['flat_type']
        flat                = address['flat']

        with sqlite3.connect(self.NEW_DB) as conn:
            cursor = conn.cursor()

            cursor.execute('''
            INSERT INTO addresses (
                unstructured_address, postal_code, country, region_type, region,
                city_district_type, city_district, city_type, city,
                street_type, street, house, block,
                flat_type, flat
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                unstructured_address, postal_code, country, region_type, region,
                city_district_type, city_district, city_type, city,
                street_type, street, house, block,
                flat_type, flat
            ))

        return f"The address has been added. | {unstructured_address}"

    def getting_addresses(self):
        with sqlite3.connect(self.dadata_db) as conn:
            cursor = conn.cursor()
            cursor.execute('select key, value from cached_data')
            rows = cursor.fetchall()
        return rows

    def run(self):
        self.create_new_database()
        addresses = self.getting_addresses()
        total_addresses = len(addresses)
        for i, (unstructured_address, data) in enumerate (addresses,1):
            print(f"{i}/{total_addresses} | {self.insert_address(unstructured_address, pickle.loads(data))}")

        print(f"That's it, the database has been created. | {self.NEW_DB}")


def main():
    if len(sys.argv) != 2:
        print(f"Using: python3 {sys.argv[0]} <db_file_name>")
        print(f"Example: python3 {sys.argv[0]} .dadata_cache.sqlite")
        sys.exit()
    
    proccessor = AddressProcessing(sys.argv[1])
    proccessor.run()

if __name__ == "__main__":
    main()
