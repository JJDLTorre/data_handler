### Toy program to test the strategy patter when dealing with data tasks
from datetime import datetime
import json
import os
import sqlite3
import pytz


class DataHandler:

    def __init__(self):
        # Get the date for the time zone America/Los_Angeles
        tz = pytz.timezone("America/Los_Angeles")
        self.today_date = datetime.now(tz).strftime("%Y-%m-%d")
        # Generate the directory name with current date
        root_dir = f"task_data/{self.today_date}"

        # Create the directory if it doesn't exist
        os.makedirs(root_dir, exist_ok=True)

        # Create sqlite db to save state
        self.state_conn = sqlite3.connect(f"{root_dir}/app_state.db")
        self.cursor = self.state_conn.cursor()
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS task_queue (
                TASK_ID INTEGER PRIMARY KEY AUTOINCREMENT, 
                TASK_DATE TEXT, 
                TASK_NAME TEXT NOT NULL, 
                TASK_DONE INTEGER DEFAULT 0, 
                TASK_DETAILS TEXT
            )
            """
        )

    def insert_task(self, data=[]):
        insert_process = """
                INSERT INTO task_queue (TASK_DATE, TASK_NAME, TASK_DETAILS)
                VALUES (?, ?, ?)
            """
        self.cursor.executemany(insert_process, data)
        self.state_conn.commit()

    def get_tasks(self, task_done=None):
        if task_done is not None:
            where_clause = f"WHERE TASK_DONE = {task_done}"
        else:
            where_clause = ""

        self.cursor.execute(
            f"""SELECT json_object(
            'TASK_ID', TASK_ID, 
            'TASK_DATE', TASK_DATE, 
            'TASK_NAME', TASK_NAME, 
            'TASK_DONE', TASK_DONE, 
            'TASK_DETAILS', TASK_DETAILS
        ) AS task_json
        FROM task_queue task_queue {where_clause}"""
        )
        result = self.cursor.fetchall()

        if result:
            tasks = [json.loads(t[0]) for t in result]
            return tasks

        return result

    def create_tasks(self):
        result = self.get_tasks()

        if result:
            return result

        # Add tasks here
        self.insert_task(data=[(self.today_date, "CONSTRUCT_TASK_QUEUE", "")])
        self.insert_task(data=[(self.today_date, "DESTRUCT_TASK_QUEUE", "")])

        result = self.get_tasks()
        return result

    def update_task_done(self, task_id, task_done=1):
        self.cursor.execute(
            f"""UPDATE task_queue SET TASK_DONE = {task_done} WHERE TASK_ID = {task_id}"""
        )
        self.state_conn.commit()


if __name__ == "__main__":
    print("hello")

    data_handler = DataHandler()
    tasks = data_handler.create_tasks()

    # Execute the tasks here
    for task in tasks:
        if task["TASK_NAME"] == "CONSTRUCT_TASK_QUEUE":
            print("Start CONSTRUCT_TASK_QUEUE")
            if task["TASK_DONE"] == 0:
                # Execute the task
                print(f"Executing task: {task}")
                data_handler.update_task_done(task["TASK_ID"])
            print("End CONSTRUCT_TASK_QUEUE")

        elif task["TASK_NAME"] == "DESTRUCT_TASK_QUEUE":
            print("Start DESTRUCT_TASK_QUEUE")
            if task["TASK_DONE"] == 0:
                # Execute the task
                print(f"Executing task: {task}")
                data_handler.update_task_done(task["TASK_ID"])
            print("End DESTRUCT_TASK_QUEUE")
