from datetime import datetime

def log_info(message):
    current_datetime = datetime.now()
    current_time = current_datetime.time()
    formatted_time = current_datetime.strftime("%H:%M:%S")
    print(f"{formatted_time} ------ {message} \n")