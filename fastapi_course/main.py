from typing import List
from fastapi import FastAPI
from models import User, Gender, Role
from uuid import uuid4

app = FastAPI()

db: List[User] = [
    User(
        id=uuid4(),
        first_name="John",
        last_name="Dauphine",
        gender=Gender.male,
        roles=[Role.admin,Role.user]
        ),
    User(
        id=uuid4(),
        first_name="Constance",
        last_name="Williams",
        gender=Gender.female,
        roles=[Role.user,Role.student]
        )
]


@app.get("/")
async def root():
    return {"Hello":"John"} 

@app.get("/api/v1/users")
async def fetch_users():
    return db

@app.post("/api/v1/users")
async def add_user(user: User):
    db.append(user)
    return {"id": user.id}