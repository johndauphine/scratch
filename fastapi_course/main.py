from typing import List
from fastapi import FastAPI,HTTPException
from models import User, Gender, Role
from uuid import uuid4,UUID

app = FastAPI()

db: List[User] = [
    User(
        id=UUID("ea438813-5340-405d-9191-b7da9b8e0c9c"),
        first_name="John",
        last_name="Dauphine",
        gender=Gender.male,
        roles=[Role.admin,Role.user]
        ),
    User(
        id=UUID("ea438813-5340-405d-9191-b7da9b8e0c9c"),
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

@app.delete("/api/v1/users/{user_id}")
async def delete_user(user_id: UUID):
    for user in db:
        if user.id == user_id:
            db.remove(user)
            return
    raise HTTPException(
        status_code=404,
        detail= f"user with id: {user_id} does not exist"
    ) 