import json
from datetime import datetime


class WorkDayTracker:
    def __init__(self, employee_name, employee_id):
        self.employee_name = employee_name
        self.employee_id = employee_id
        self.day_start_time = datetime.now()
        self.day_end_time = None
        self.daily_tasks = []
        self.active_task = None

    def begin_day(self):
        self.day_start_time = datetime.now()
        print(f"{self.employee_name} began their day at {self.day_start_time}")

    def initiate_task(self, title, details):
        self.active_task = {
            "task_title": title,
            "task_description": details,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "end_time": None,
            "task_successful": False
        }
        print(f"Task started: {title} at {self.active_task['start_time']}")

    def conclude_task(self, success):
        if self.active_task:
            self.active_task["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            self.active_task["task_successful"] = success
            self.daily_tasks.append(self.active_task)
            print(f"Task ended: {self.active_task['task_title']} at {self.active_task['end_time']}")
            self.active_task = None
        else:
            print("No task is currently active.")

    def finish_day(self):
        self.day_end_time = datetime.now()
        print(f"{self.employee_name} finished their day at {self.day_end_time}")

    
        day_summary = {
            "employee_name": self.employee_name,
            "employee_id": self.employee_id,
            "day_start_time": self.day_start_time.strftime("%Y-%m-%d %H:%M"),
            "day_end_time": self.day_end_time.strftime("%Y-%m-%d %H:%M"),
            "tasks": self.daily_tasks
        }

        file_name = f"{self.employee_name}_{self.day_start_time.strftime('%Y-%m-%d')}.json"
        with open(file_name, 'w') as json_file:
            json.dump(day_summary, json_file, indent=4)
        print(f"Day's work saved to {file_name}")



if __name__ == "__main__":
    employee_name = input("Enter employee name: ")
    employee_id = int(input("Enter employee ID: "))

    employee = WorkDayTracker(employee_name=employee_name, employee_id=employee_id)
    employee.begin_day()

    while True:
        task_title = input("Enter task title (or type 'finish' to end the day): ")
        if task_title.lower() == 'finish':
            break
        task_description = input("Enter task description: ")
        employee.initiate_task(task_title, task_description)

        task_success = input("Was the task successful? (yes/no): ").strip().lower() == 'yes'
        employee.conclude_task(task_success)

    employee.finish_day()
