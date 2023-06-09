from datetime import timedelta
from models import session, TaskChecker


def task_checker_db(status, task_details, comment='', task_name='Get EMSX Trade', task_type='Task Scheduler ', only_new=False):
    if comment != '':
        comment_db = comment
    else:
        comment_db = 'Success'
    add_db = True
    if only_new:
        my_task = session.query(TaskChecker).filter(TaskChecker.task_details == task_details)\
                                            .filter(TaskChecker.status == status)\
                                            .filter(TaskChecker.active == 1).first()
        if my_task:
            add_db = False
    if add_db:
        new_task_checker = TaskChecker(
            task_name=task_name,
            task_details=task_details,
            task_type=task_type,
            status=status,
            comment=comment_db
        )
        session.add(new_task_checker)
        session.commit()

    if status == 'Success':
        session.query(TaskChecker).filter(TaskChecker.task_details == task_details) \
            .filter(TaskChecker.status == 'Fail').filter(TaskChecker.active == 1).delete()
        session.commit()


def clean_df_value(serie):
    if not serie.empty:
        return serie[0]
    else:
        return None


def find_future_date(my_date, days):
    cur_date = my_date
    nb_days = days
    while nb_days > 0:
        cur_date += timedelta(days=1)
        if cur_date.weekday() < 5:
            nb_days -= 1
    return cur_date


def find_past_date(my_date, days):
    cur_date = my_date
    nb_days = days
    while nb_days > 0:
        cur_date -= timedelta(days=1)
        if cur_date.weekday() < 5:
            nb_days -= 1
    return cur_date


def get_df_des(df):
    df_return = df[['a_1d', 'a_2d', 'a_3d', 'a_1w',
        'a_2w', 'a_4w', 'a_8w']].describe()
    df_return['Desc'] = df_return.index
    df_return = df_return[['Desc', 'a_1d', 'a_2d', 'a_3d', 'a_1w',
        'a_2w', 'a_4w', 'a_8w']]

    hr_1d = df[(df['a_1d'] >= 0)]['a_1d'].count() / df['a_1d'].count()
    hr_2d = df[(df['a_2d'] >= 0)]['a_2d'].count() / df['a_2d'].count()
    hr_3d = df[(df['a_3d'] >= 0)]['a_3d'].count() / df['a_3d'].count()
    hr_1w = df[(df['a_1w'] >= 0)]['a_1w'].count() / df['a_1w'].count()
    hr_2w = df[(df['a_2w'] >= 0)]['a_2w'].count() / df['a_2w'].count()
    hr_4w = df[(df['a_4w'] >= 0)]['a_4w'].count() / df['a_4w'].count()
    hr_8w = df[(df['a_8w'] >= 0)]['a_8w'].count() / df['a_8w'].count()

    df_return.loc['HitRatio'] = ['HitRatio', hr_1d, hr_2d, hr_3d, hr_1w, hr_2w, hr_4w, hr_8w]

    return df_return


def append_excel(df_des, title, ws):
    ws.append(title)
    headers = df_des.columns.tolist()
    rows = df_des.values.tolist()
    ws.append(headers)

    for row in rows:
        ws.append(row)