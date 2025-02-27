import time

def get_user():

    return 'ADAM'

def run_program(ti):

    user = ti.xcom_pull(task_ids='GETUSER', key='return_value')
    
    print(f'Running the program for user : {user}')

    time.sleep(5)

    print('Program executed sucessufully!')