from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy

from datetime import datetime
from db_url import db_url

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


# ToDoList
#
# id                INT
# name              VARCHAR
# description       VARCHAR(100)
# done              BOOLEAN


class Task(db.Model):
    __table_args__ = {"schema":"todolist_schema"}
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    description = db.Column(db.String(100))
    is_done = db.Column(db.Boolean, default=False)
    time_added = db.Column(db.Date, default=datetime.utcnow)

    def __repr__(self):
        return f'<Task: {self.email}>'


@app.get("/task") # http://127.0.0.1:5000/task
def get_tasks():
    tasks = []
    tasks_data = Task.query.all()
    for task_data in tasks_data:
        task = {
            'id': task_data.id,
            'name': task_data.name,
            'description': task_data.description,
            'is_done': task_data.is_done,
            'time_added': task_data.time_added
        }
        
        tasks.append(task)

    return {"tasks": tasks}

@app.get("/task/<int:task_id>") 
def get_task(task_id):
    task = Task.query.filter_by(id=task_id).first()
    task = {
        'id': task.id,
        'name': task.name,
        'description': task.description,
        'is_done': task.is_done,
        'time_added': task.time_added
    }
    return task

@app.post("/task")
def create_task():
    task_data = request.get_json()
    task = Task(**task_data)
    db.session.add(task)
    db.session.commit()

    task = {
        'id': task.id,
        'name': task.name,
        'description': task.description,
        'is_done': task.is_done,
        'time_added': task.time_added
    }

    return task

@app.delete("/task/<int:task_id>")
def delete_task(task_id):
    task = Task.query.filter_by(id=task_id).first()
    db.session.delete(task)
    db.session.commit()

    task = {
        'id': task.id,
        'name': task.name,
        'description': task.description,
        'is_done': task.is_done,
        'time_added': task.time_added
    }

    return task

@app.put("/task/<int:task_id>")
def update_task(task_id):
    task_data = request.get_json()
    task = Task.query.filter_by(id=task_id).update(task_data)   # task |= task_data
    db.session.commit()
    
    task = Task.query.filter_by(id=task_id).first()
    task = {
        'id': task.id,
        'name': task.name,
        'description': task.description,
        'is_done': task.is_done,
        'time_added': task.time_added
    }

    return task

# Create db
with app.app_context():
    db.create_all()