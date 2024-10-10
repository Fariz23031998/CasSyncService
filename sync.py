import pyodbc
import fdb
from datetime import datetime
import time

import pywintypes
import win32api


def write_log_file(text):
    with open("log.txt", "a", encoding='utf-8') as file:
        file.write(text + '\n')


with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())


price_type = config["price_type"]
stock_house = config["object_id"]
host = config["host"]
database = config["database"]
user = config["user"]
password = config["password"]
mdb_conn = config["mdb_conn"]
divider = config["divider"]
weight_unit_id = config["weight_unit_id"]
piece_unit_id = config["piece_unit_id"]
use_piece = config["use_piece"]
departments = config["departments"]
check_time = config["check_time"]


class UpdateData:
    def __init__(self):
        # ... (other initialization code)
        self.fdb_conn = None
        self.fdb_cursor = None
        self.last_sync = 0
        with open("log.txt", 'w', encoding='utf-8') as file:
            file.write(f"File created at {self.get_date()}\n")

        self.path = self.get_short_path_name(database)
        self.mdb_conn = None
        self.mdb_cursor = None

    def connect_fdb(self):
        try:
            self.fdb_conn = fdb.connect(
                host=host,
                database=self.path,
                user=user,
                password=password,
                charset='utf-8',
            )
        except fdb.fbcore.DatabaseError:
            write_log_file(f"Не получается к база данных Regos. {self.get_date()}")
            return False
        else:
            self.fdb_cursor = self.fdb_conn.cursor()
            self.connect_mdb()
            return True

    def connect_mdb(self):
        try:
            self.mdb_conn = pyodbc.connect(mdb_conn)
        except pyodbc.Error as e:
            write_log_file(f"Не получается к база данных CL-Works Pro: {e} ({self.get_date()})")
            return False
        else:
            self.mdb_cursor = self.mdb_conn.cursor()
            return True

    def get_short_path_name(self, path):
        try:
            return win32api.GetShortPathName(path)
        except pywintypes.error:
            return path

    def get_date(self):
        now = datetime.now()
        return now.strftime("%m/%d/%Y %H:%M:%S")

    def check_cash_status(self):
        query = "SELECT SST_DATE, SST_STATUS " \
                "FROM SYS_SYNC_PROCCESS_REF " \
                "WHERE SST_STATUS = 1"
        try:
            self.fdb_cursor.execute(query)
        except AttributeError:
            return 404
        sync_process = self.fdb_cursor.fetchall()
        sync_value = 0
        for sync in sync_process:
            timestamp = sync[0].timestamp()
            if timestamp > sync_value:
                sync_value = timestamp

        if sync_value > self.last_sync:
            self.last_sync = sync_value
            return 200
        else:
            return 201

    def update_departments(self):
        mdb_query = """
        SELECT Code, Speedkey, DeptName FROM TbDepartment
        """

        update_query = """
        UPDATE TbDepartment SET DeptName = ?, Speedkey = ?
        WHERE Code = ?
        """

        insert_query = """
        INSERT INTO TbDepartment (Code, Speedkey, DeptName) 
        VALUES (?, ?, ?)
        """

        try:
            self.mdb_cursor.execute(mdb_query)
            mdb_departments = {dep[0]: dep for dep in self.mdb_cursor.fetchall()}

            updates = []
            inserts = []
            for dep_key, dep_value in departments.items():
                if dep_value[0] in mdb_departments:
                    if mdb_departments[dep_value[0]][1] != dep_value[0] or mdb_departments[dep_value[0]][2] != dep_value[1]:
                        updates.append((dep_value[1], dep_value[0], dep_value[0]))
                else:
                    inserts.append((dep_value[0], dep_value[0], dep_value[1]))

            # Execute batch updates
            if updates:
                self.mdb_cursor.executemany(update_query, updates)

            # Execute batch inserts
            if inserts:
                self.mdb_cursor.executemany(insert_query, inserts)

            # Commit the transaction
            self.mdb_conn.commit()

        except (pyodbc.Error, fdb.Error) as e:
            if self.mdb_conn:
                self.mdb_conn.rollback()
            write_log_file(f"Error updating departments: {e} ({self.get_date()})")

        else:
            write_log_file(f"Updated {len(updates)} departments and inserted {len(inserts)} new departments. ({self.get_date()})")

    def update_groups(self):
        # Query to get groups from Firebird
        fdb_query = """
        SELECT ITMG_ID, ITMG_NAME, ITMG_DELETED FROM CTLG_ITM_GROUPS_REF
        WHERE ITMG_DELETED=0
        """
        mdb_query = """
        SELECT Code, GroupName FROM TbGroup
        """

        update_query = """
        UPDATE TbGroup
        SET GroupName = ?
        WHERE Code = ?
        """

        insert_query = """
        INSERT INTO TbGroup (Code, GroupName)
        VALUES (?, ?)
        """
        try:
            # Fetch data from Firebird
            self.fdb_cursor.execute(fdb_query)
            fdb_groups = {group[0]: group for group in self.fdb_cursor.fetchall()}

            # Fetch data from access
            self.mdb_cursor.execute(mdb_query)
            mdb_groups = {group[0]: group for group in self.mdb_cursor.fetchall()}

            updates = []
            inserts = []

            for group_code, group_row in fdb_groups.items():
                group_name = group_row[1]

                if group_code in mdb_groups:
                    if group_name != mdb_groups[group_code][1]:
                        updates.append((group_code, group_name))
                else:
                    inserts.append((group_code, group_name))

            # Execute batch updates
            if updates:
                self.mdb_cursor.executemany(update_query, updates)

            # Execute batch inserts
            if inserts:
                self.mdb_cursor.executemany(insert_query, inserts)

            # Commit the transaction
            self.mdb_conn.commit()

        except (pyodbc.Error, fdb.Error) as e:
            if self.mdb_conn:
                self.mdb_conn.rollback()
            write_log_file(f"Error updating groups: {e} ({self.get_date()})")

        else:
            write_log_file(f"Updated {len(updates)} groups and inserted {len(inserts)} new groups. ({self.get_date()})")

    def update_items(self):
        # Query to get items from Firebird
        fdb_query = """
        SELECT f.ITM_ID, f.ITM_CODE, f.ITM_NAME, f.ITM_UNIT, f.ITM_GROUP, p.PRC_VALUE
        FROM CTLG_ITM_ITEMS_REF f
        JOIN CTLG_ITM_PRICES_REF p ON f.ITM_ID = p.PRC_ITEM
        WHERE f.ITM_DELETED_MARK = 0 AND p.PRC_PRICE_TYPE = ? AND p.PRC_VALUE <> 0
        """


        # Query to get existing items from Access
        mdb_query = """
        SELECT PluNo, PluType, Name1, UnitPrice, GroupNo, DeptNo
        FROM TbPLU
        """

        # Queries for updating and inserting in Access
        update_query = """
        UPDATE TbPLU 
        SET PluType = ?, Name1 = ?, UnitPrice = ?, UpdateDate = ?, GroupNo = ?, DeptNo = ?
        WHERE PluNo = ?
        """

        insert_query = """
        INSERT INTO TbPLU (PluNo, PluType, ItemCode, Name1, UnitPrice, UpdateDate, GroupNo, DeptNo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            # Fetch data from Firebird
            self.fdb_cursor.execute(fdb_query, (price_type,))
            fdb_items = {item[1]: item for item in self.fdb_cursor.fetchall()}  # Using ITM_CODE as key

            # Fetch data from Access
            self.mdb_cursor.execute(mdb_query)
            mdb_items = {item[0]: item for item in self.mdb_cursor.fetchall()}  # Using PluNo as key

            updates = []
            inserts = []
            current_date = self.get_date()

            for itm_code, fdb_item in fdb_items.items():
                plu_type = 1 if fdb_item[3] == weight_unit_id else 3
                name = fdb_item[2][:64]
                unit_price = float(fdb_item[5] / divider)
                group_no = fdb_item[4]
                dept_no = departments["W"][0] if plu_type == 1 else departments["P"][0]

                if itm_code in mdb_items:
                    mdb_item = mdb_items[itm_code]
                    if (plu_type != mdb_item[1] or
                            name != mdb_item[2] or
                            abs(unit_price - mdb_item[3]) > 0.5 or
                            group_no != mdb_item[4] or dept_no != mdb_item[5]):
                        updates.append((plu_type, name, unit_price, current_date, group_no, dept_no, itm_code))
                else:
                    inserts.append((itm_code, plu_type, itm_code, name, unit_price, current_date, group_no, dept_no))
            # Execute batch updates
            if updates:
                self.mdb_cursor.executemany(update_query, updates)

            # Execute batch inserts
            if inserts:
                self.mdb_cursor.executemany(insert_query, inserts)

            # Commit the transaction
            self.mdb_conn.commit()

        except (pyodbc.Error, fdb.Error) as e:
            if self.mdb_conn:
                self.mdb_conn.rollback()
            write_log_file(f"Error updating items: {e} ({self.get_date()})")

        else:
            write_log_file(f"Updated {len(updates)} items and inserted {len(inserts)} new items. ({self.get_date()})")


update_data = UpdateData()
update_data.connect_fdb()
update_data.connect_mdb()

while True:
    cash_status = update_data.check_cash_status()
    if cash_status == 200:
        update_data.update_departments()
        update_data.update_groups()
        update_data.update_items()
    elif cash_status == 201:
        pass
    else:
        update_data.connect_fdb()
        update_data.connect_mdb()
        time.sleep(check_time)
