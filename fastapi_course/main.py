from typing import List
from fastapi import FastAPI,HTTPException
from models import User, Gender, Role,UserUpdateRequest
from uuid import uuid4,UUID
import uvicorn

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
        id=UUID("589872f7-4f12-4d6b-9c4f-df9fda2be594"),
        first_name="Constance",
        last_name="Williams",
        gender=Gender.female,
        roles=[Role.user,Role.student]
        )
]


@app.get("/")
async def root():
    return {"message":"Hello Chase!"} 

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

@app.put("/api/v1/users/{user_id}")
async def update_user(user_update: UserUpdateRequest,user_id: UUID):
    for user in db:
        if user.id == user_id:
            if user_update.first_name is not None:
                user.first_name=user_update.first_name
            if user_update.last_name is not None:
                user.last_name=user_update.last_name
            if user_update.middle_name is not None:
                user.middle_name=user_update.middle_name
            if user_update.roles is not None:
                user.roles=user_update.roles
            return
    raise HTTPException(
        status_code=404,
        detail= f"user with id: {user_id} does not exist"
    ) 

if __name__=="__main__":
    try:
        uvicorn.run("main:app",host="0.0.0.0", port=8000,reload=True)
    except Exception as exc:
        print(exc)