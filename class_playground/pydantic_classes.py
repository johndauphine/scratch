
from datetime import date,datetime
from typing import List
from pydantic import BaseModel, ValidationError,validator
from enum import Enum


class Gender(str,Enum):
    male = "male"
    female = "female"

class Person(BaseModel):
    first_name:str
    last_name:str
    birth_date:date
    gender:Gender
    
    @validator('first_name')
    def check_first_name(cls,value:str):
        if value.isdigit():
            raise ValueError("First name must be a character value")
        return value

    @validator('last_name')
    def check_last_name(cls,value:str):
        if value.isdigit():
            raise ValueError("Last name must be a character value")
        return value



class Employee(Person):
    salary:int






person1 = Person( first_name='John',last_name='Dauphine',
                 birth_date=date(1969,11,25),gender=Gender.male)

employee1 = Employee(first_name='Connie',last_name='666',
                     birth_date=date(1969,8,14),gender=Gender.female,salary=50000)



people:List[Person] = [person1,employee1]

for p in people:
    print(p.first_name)

print(employee1)