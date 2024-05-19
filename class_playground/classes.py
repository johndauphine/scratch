
from datetime import date,datetime

class Person:
    def __init__(self,first_name:str,last_name:str,birth_date:date):
        self.first_name = first_name
        self.last_name = last_name
        self.birth_date = birth_date
        self.age = self.get_age()

    def __repr__(self) -> str:
        return f"Person('{self.first_name}','{self.last_name}','{self.birth_date}')"

    def __str__(self) -> str:
        return f"Name:{self.first_name} {self.last_name} Age:{self.age}"
    
    def get_age(self):
        today = date.today()
        return today.year - self.birth_date.year - ((today.month, today.day) < (self.birth_date.month, self.birth_date.day))

class Employee(Person):
    def __init__(self, first_name, last_name, birth_date,salary):
        super().__init__(first_name, last_name, birth_date)
        self.salary = salary

    def __repr__(self) -> str:
        return f"Employee('{self.first_name}','{self.last_name}','{self.birth_date}',{self.salary})"

    def __str__(self) -> str:
        return f"Name:{self.first_name} {self.last_name} Age:{self.age} Salary:{self.salary}"





person1 = Person('John','Dauphine',date(1969,11,25))

person2 = Person('Constance','Williams',date(1969,8,14))

people = [person1,person2]

for p in people:
    print(p)
