
from datetime import date,datetime

from pydantic import BaseModel
from enum import Enum


class Gender(str,Enum):
    male = "male"
    female = "female"

class Person(BaseModel):
    first_name:str
    last_name:str
    birth_date:date
    gender:Gender
  

class Employee(Person):
    first_name:str
    last_name:str
    birth_date:date
    salary:int
    gender:Gender





person1 = Person( first_name='John',last_name='Dauphine',birth_date=date(1969,11,25),gender=Gender.male)

person2 = Person(first_name='Connie',last_name='Williams',birth_date=date(1969,8,14),gender=Gender.female)

people = [person1,person2]

for p in people:
    print(p.first_name)

print(person1)