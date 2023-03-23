
from datetime import date,datetime

class Person:
    def __init__(self,first_name,last_name,birth_date):
        self.first_name = first_name
        self.last_name = last_name
        self.birth_date = birth_date
        self.age = self.calculate_age(datetime.strptime(birth_date,'%Y-%m-%d' ))

    def __repr__(self) -> str:
        return f"Person('{self.first_name}','{self.last_name}','{self.birth_date}')"

    def __str__(self) -> str:
        return f"Name:{self.first_name} {self.last_name} DOB:{self.birth_date}"
    
    def calculate_age(self,born):
        today = date.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

class Employee(Person):
    def __init__(self, first_name, last_name, birth_date,salary):
        super().__init__(first_name, last_name, birth_date)
        self.salary = salary

    def __repr__(self) -> str:
        return f"Employee('{self.first_name}','{self.last_name}','{self.birth_date}',{self.salary})"

    def __str__(self) -> str:
        return f"Name:{self.first_name} {self.last_name} DOB:{self.birth_date} Salary:{self.salary}"





person1 = Person('John','Dauphine','1969-11-25')

employee1 = Employee('Connie','Williams','1969-08-14',50000)

print(person1)
print(person1.age)

print(employee1)