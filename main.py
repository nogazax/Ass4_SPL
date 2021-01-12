import sqlite3
import os


class Clinic:
    def __init__(self, id, location, demand, logistics):
        self.id = id
        self.location = location
        self.demand = demand
        self.logistics = logistics


class _Clinics:
    def __init__(self, conn):
        self._conn = conn

    def insert_clinic(self, Clinic):
        self._conn.execute("""
        INSERT INTO clinics (id, location, demand, logistic) VALUES (?, ?, ?, ?)
                """, [Clinic.id, Clinic.location, Clinic.demand, Clinic.logistics]
                           )


class Vaccine:
    def __init__(self, id, date, supplier, quantity):
        self.id = id
        self.date = date
        self.supplier = supplier
        self.quantity = quantity


class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert_vaccine(self, vaccine):
        self._conn.execute("""
        INSERT INTO vaccines (id, date, supplier, quantity) VALUES (?, ?, ?, ?)
                """, [vaccine.id, vaccine.date, vaccine.supplier, vaccine.quantity]
                           )


class Logistic:
    def __init__(self, id, name, count_sent, count_received):
        self.id = id
        self.name = name
        self.count_sent = count_sent
        self.count_received = count_received


class _Logistics:
    def __init__(self, conn):
        self._conn = conn

    def insert_logistic(self, logistic):
        self._conn.execute("""
        INSERT INTO logistics (id, name, count_sent, count_received) VALUES (?, ?, ?, ?)
        """, [logistic.id, logistic.name, logistic.count_sent, logistic.count_received])


class Supplier:
    def __init__(self, id, name, logistic):
        self.id = id
        self.name = name
        self.logistic = logistic


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert_supplier(self, supplier):
        self._conn.execute("""
           INSERT INTO suppliers (id, name, logistic) VALUES (?, ?, ?)
           """, [supplier.id, supplier.name, supplier.logistic])


class Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = _Vaccines(self._conn)
        self.logistics = _Logistics(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.clinics = _Clinics(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
            
              create table logistics (
                    id      INTEGER     PRIMARY KEY,
                    name    TEXT        NOT NULL,
                    count_sent    INTEGER     NOT NULL,
                    count_received    INTEGER NOT NULL
                );
            
              CREATE TABLE vaccines (
                  id    INTEGER         PRIMARY KEY,
                  date  DATE        NOT NULL,
                  supplier INTEGER ,           
                  quantity    INTEGER        NOT NULL,
                  FOREIGN KEY(supplier) REFERENCES suppliers(id)
              );

              CREATE TABLE clinics (
                  id          INTEGER     PRIMARY KEY,
                  location    TEXT    NOT NULL,
                  demand      INTEGER  NOT NULL,
                  logistic    INTEGER,
                   FOREIGN KEY(logistic) REFERENCES logistics(id)
              );
             

              CREATE TABLE suppliers (
                  id      INTEGER     PRIMARY KEY,
                  name  INTEGER     NOT NULL,
                  logistic   INTEGER,
                   FOREIGN KEY(logistic) REFERENCES logistics(id)
                   
              );
                 """)

    def insert_vaccine(self, vac):
        self.vaccines.insert_vaccine(vac)

    def insert_supplier(self, supp):
        self.suppliers.insert_supplier(supp)

    def insert_logistic(self, log):
        self.logistics.insert_logistic(log)

    def insert_clinic(self, clinic):
        self.clinics.insert_clinic(clinic)

    def get_all_vaccines(self):
        c = self._conn.cursor()
        c.execute("""
        SELECT * FROM vaccines
        """)

        return [Vaccine(*d) for d in c.fetchall()]

    def get_new_vac_id(self):
        c = self._conn.cursor()
        c.execute("""
                SELECT id FROM vaccines
                """)

        lst = [*c.fetchall()]
        return int(*lst[-1]) + 1

    def get_all_sups(self):
        c = self._conn.cursor()
        c.execute("""
        SELECT * FROM suppliers
        """)
        return [Supplier(*d) for d in c.fetchall()]

    def get_logID_from_supp_name(self, supp_name):
        c = self._conn.cursor()
        c.execute("""
                SELECT logistic FROM suppliers WHERE name = (?)
                """, [supp_name])
        ans = int(*c.fetchone())
        return ans

    def add_received_vaccines_to_logistic(self, log_id, amount):
        curr_amount = self.get_curr_amount_received(log_id)
        new_amount = int(curr_amount) + amount
        self._conn.execute("""
            UPDATE logistics SET count_received=(?) WHERE id=(?)
        """, [new_amount, log_id])

    def add_sent_vaccines_to_logistic(self, clinic_loc, amount_to_add):
        log_id = self.get_logID_from_clinic_name(clinic_loc)
        curr_amount = self.get_curr_amount_send(log_id)
        self._conn.execute("""
                    UPDATE logistics SET count_sent=(?) WHERE id=(?)
                """, [curr_amount + amount_to_add, log_id])

    def get_curr_amount_send(self, log_id):
        c = self._conn.cursor()
        c.execute("""
                                SELECT count_sent FROM logistics WHERE id = (?)
                                """, [log_id])
        ans = int(*c.fetchone())
        return ans

    def get_logID_from_clinic_name(self, clinic_loc):
        c = self._conn.cursor()
        c.execute("""
                        SELECT logistic FROM clinics WHERE location = (?)
                        """, [clinic_loc])
        ans = int(*c.fetchone())
        return ans

    def get_curr_amount_received(self, log_id):
        c = self._conn.cursor()
        c.execute("""
                        SELECT count_received FROM logistics WHERE id = (?)
                        """, [log_id])
        ans = int(*c.fetchone())
        return ans

    def reduce_inventory_amount(self, amount):
        while amount > 0:
            c = self._conn.cursor()
            c.execute("""
            SELECT * FROM vaccines ORDER BY date(date) DESC Limit 1
            """)
            vac_attribute = c.fetchone()
            if vac_attribute[3] <= amount:  # amount to reduce is bigger than quantity of this batch
                amount = amount - vac_attribute[3]
                self._conn.execute("""
                DELETE FROM vaccines WHERE id=(?)
            """, [vac_attribute[0]])
            else:  # amount is smaller than the storage of this batch
                new_amount = vac_attribute[3] - amount
                amount = 0
                self._conn.execute("""
                            UPDATE vaccines SET quantity=(?) WHERE id=(?)
                        """, [new_amount, vac_attribute[0]])

    def reduce_clinic_demand(self, clinic_name, amount_to_reduce):
        c = self._conn.cursor()
        c.execute("""
                   SELECT demand FROM clinics WHERE location=(?)
                   """, [clinic_name])
        fetchedobj = [*c.fetchone()]
        curr_demand = fetchedobj[0]
        self._conn.execute("""
            UPDATE clinics SET demand=(?) WHERE location=(?)
        """, [curr_demand - amount_to_reduce, clinic_name])

    def get_all_clinic_demands(self):
        c = self._conn.cursor()
        c.execute("""
                           SELECT demand FROM clinics
                           """)
        all_demands = c.fetchall()
        all_demands = [d[0] for d in all_demands]
        total_demand = 0
        for demand in all_demands:
            total_demand += demand
        return total_demand

    def get_total_inventory(self):
        c = self._conn.cursor()
        c.execute(""" 
            SELECT quantity FROM vaccines
        """)
        total_inv = c.fetchall()
        total_inv = [d[0] for d in total_inv]
        summary = 0
        for curr in total_inv:
            summary += curr
        return summary

    def get_all_total_recieved(self):
        c = self._conn.cursor()
        c.execute(""" 
                  SELECT count_received FROM logistics
              """)
        total_rec = c.fetchall()
        total_rec = [d[0] for d in total_rec]
        summary = 0
        for curr in total_rec:
            summary += curr
        return summary

    def get_all_total_sent(self):
        c = self._conn.cursor()
        c.execute(""" 
                         SELECT count_sent FROM logistics
                     """)
        total_sent = c.fetchall()
        total_sent = [d[0] for d in total_sent]
        summary = 0
        for curr in total_sent:
            summary += curr
        return summary


def update_log(outputFile, repository):
    total_inventory = repository.get_total_inventory()
    total_demand = repository.get_all_clinic_demands()
    total_received = repository.get_all_total_recieved()
    total_sent = repository.get_all_total_sent()
    outputFile.write("{},{},{},{}\n".format(total_inventory, total_demand, total_received, total_sent))


# <total_inventory>,<total_demand>,<total_received>,<total_sent>

def handle_orders(repo):
    inputFile = open('orders.txt')
    outputFile = open('output.txt', 'w')
    order_Lines = inputFile.read().split('\n')
    for line in order_Lines:
        order = line.split(",")
        if len(order) == 2:
            vaccine_to_clinic(order, repo)
        else:
            vaccine_to_inventory(order, repo)
        repo._conn.commit()
        update_log(outputFile, repo)


def vaccine_to_clinic(order, repo):
    amount = int(order[1])
    repo.reduce_inventory_amount(amount)
    repo.reduce_clinic_demand(order[0], amount)
    repo.add_sent_vaccines_to_logistic(order[0], amount)


def vaccine_to_inventory(order, repository):
    # get logistic_id -> update logistics with new amount received -> insert new vaccine to vaccines table
    log_id = repository.get_logID_from_supp_name(order[0])
    repository.add_received_vaccines_to_logistic(log_id, int(order[1]))
    repository.insert_vaccine(Vaccine(repository.get_new_vac_id(), order[2], order[0], order[1]))


def read_conf_file_to_database(repo):
    inputFile = open('config.txt')
    numLine = inputFile.readline().replace('\n', "")
    nums = numLine.split(',')
    nums = [int(n) for n in nums]
    lines = inputFile.read().split('\n')
    # TODO make readable
    vac_lines = lines[:nums[0]]
    sup_lines = lines[nums[0]:nums[0] + nums[1]]
    clinic_lines = lines[nums[0] + nums[1]:nums[0] + nums[1] + nums[2]]
    log_lines = lines[nums[0] + nums[1] + nums[2]:]
    insert_to_DB(log_lines, repo.insert_logistic, Logistic)
    insert_to_DB(clinic_lines, repo.insert_clinic, Clinic)
    insert_to_DB(sup_lines, repo.insert_supplier, Supplier)
    insert_to_DB(vac_lines, repo.insert_vaccine, Vaccine)


def insert_to_DB(lines, insert_function, type):
    for v_line in lines:
        v_params = v_line.split(',')
        v = type(*v_params)
        insert_function(v)


def main():
    os.remove('database.db')
    repo = Repository()
    repo.create_tables()
    read_conf_file_to_database(repo)
    handle_orders(repo)
    repo._close()
    # #  print(repo.get_new_vac_id())
    # #  print(repo.get_logID_from_supp_name('Moderna'))
    # orderTest = "Jerusalem,100".split(",")
    # # vaccine_to_inventory(orderTest,repo)
    # print(repo.get_all_clinic_demands())
    # vaccine_to_clinic(orderTest, repo)
    # print(repo.get_all_clinic_demands())
    # print(repo.get_curr_amount_received(1))
    #
    # repo.add_received_vaccines_to_logistic(1, 20)
    # print(repo.get_curr_amount_received(1))
    # repo.add_received_vaccines_to_logistic(1, 30)
    # print(repo.get_curr_amount_received(1))
    #
    # print(repo.get_all_clinic_demands())


if __name__ == '__main__':
    main()
    # ======================================================================================= test code
