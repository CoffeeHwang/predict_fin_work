"""
yhs mysql 테이블 불러오기 함수화
"""

from pymysql import cursors, connect
import pandas as pd
from typing import Union
from datetime import datetime, timedelta
from yhs_common import *


def open_yhsdb_conn(user: str, pw: str, database: str, charset: str = 'utf8', host: str = None, unix_socket: str = None) -> Union[connect, None]:
    try:
        conn = connect(
            user=user,
            password=pw,
            unix_socket=unix_socket,
            host=host,
            db=database,
            charset=charset
        )
    except Exception as e:
        print(e)
        return None
    else:
        return conn


def close_yhsdb_conn(conn: connect):
    conn.close()


def __get_yhsdb_query(conn: connect, sql: str, args: tuple) -> pd.DataFrame:
    cursor = conn.cursor(cursors.DictCursor)
    try:
        cursor.execute(query=sql, args=args)
    except Exception as e:
        print(e)
        log(f"get sql error = {sql} {args}")
        result = None
    else:
        result = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(result)


def __set_yhsdb_query(conn: connect, sql: str, args: tuple) -> int:
    cursor = conn.cursor(cursors.DictCursor)
    try:
        execute_row_cnt = cursor.execute(query=sql, args=args)
    except Exception as e:
        print(e)
        log(f"set sql error = {sql} {args}")
        return 0
    else:
        cursor.close()
        return execute_row_cnt


def commit(conn: connect):
    conn.commit()


def rollback(conn: connect):
    conn.rollback()


def sel_ref_worktime(conn: connect, ent_id: int) -> pd.DataFrame:
    sql = "select ent_id, time_mng_type_cd, start_time, end_time " \
          "from ref_std_worktime " \
          "where ent_id = %s " \
          "order by start_time asc;"
    return __get_yhsdb_query(conn=conn, sql=sql, args=(ent_id,))


def sel_ref_std_worktime(conn: connect, ent_id: int) -> tuple:
    """
    표준근로 시간을 반환
    :param conn:
    :param ent_id:
    :return: 튜플 형태의 표준근로 시간(str) - (시작시간, 종료시간)
    """
    df: pd.DataFrame = sel_ref_worktime(conn=conn, ent_id=ent_id)
    if len(df) > 0:
        df = df.loc[df.time_mng_type_cd == 'STD', ['start_time', 'end_time']]
        return df.iloc[0]['start_time'], df.iloc[0]['end_time']
    else:
        return None, None


def sel_ref_brk_time(conn: connect, ent_id: int) -> list:
    df: pd.DataFrame = sel_ref_worktime(conn=conn, ent_id=ent_id)
    if len(df) > 0:
        df = df.loc[df.time_mng_type_cd == 'BRK', ['start_time', 'end_time']]
        return [tuple(v) for v in df.values]
    else:
        return []


def sel_process_hist(conn: connect, lot: int) -> pd.DataFrame:
    sql = "select seq, ent, mkey, lot, mid, program, plan, count, start, end " \
          "from process_hist where lot = %s " \
          "order by start asc;"
    return __get_yhsdb_query(conn=conn, sql=sql, args=(lot,))


def set_predict_end_on_process_hist(conn: connect, seq: int, lot: int, predict_end_time: datetime) -> int:
    """
    :param conn:
    :param seq:
    :param lot:
    :param predict_end_time: 예측 종료 시간
    :return: 업데이트 성공 row 수
    """
    sql = "update process_hist set prdct_end = %s where seq = %s and lot = %s;"
    return __set_yhsdb_query(conn=conn, sql=sql, args=(predict_end_time, seq, lot))


def sel_ref_holidays(conn: connect, std_worktime_start: str, std_worktime_end: str,
                     ent_id: int, mkey: int,
                     start_datetime: datetime, end_datetime: datetime) -> pd.DataFrame:
    """
        지정한 시작~종료일 범위의 공휴일 데이터프레임을 반환
    :param conn:
    :param std_worktime_start: 표준근로 시작시간
    :param std_worktime_end: 표준근로 종료시간
    :param ent_id: 업체id
    :param mkey: 장비고유번호
    :param start_datetime: 근로시작시간
    :param end_datetime: 근로종료시간(범위)
    :return:
    """
    start_date = start_datetime.strftime("%Y-%m-%d")
    end_date = end_datetime.strftime("%Y-%m-%d")
    sql = f"select concat(holiday, ' ', %s) start_time, concat(holiday, ' ' , %s) end_time, holiday_desc comment " \
          f"from ref_holiday " \
      f"where holiday between %s and %s " \
          f"union all " \
          f"select start_time, end_time, comment " \
          f"from ref_add_brktime " \
          f"where ent_id = %s and mkey = %s and start_time >= %s and end_time <= %s " \
          f"order by 1;"

    return __get_yhsdb_query(conn=conn, sql=sql, args=(std_worktime_start, std_worktime_end, start_date, end_date, ent_id, mkey, start_datetime, end_datetime))


def sel_ref_add_worktime(conn: connect, ent_id: int, mkey: int, work_start_firsttime: datetime, work_end_predicttime: datetime) -> list:
    sql = f"select start_time, end_time, comment " \
          f"from ref_add_worktime " \
          f"where ent_id = %s and mkey = %s and start_time >= %s and end_time <= %s " \
          f"order by 1;"
    df: pd.DataFrame = __get_yhsdb_query(conn=conn, sql=sql, args=(ent_id, mkey, work_start_firsttime, work_end_predicttime))

    if len(df) > 0:
        return [(v[0].strftime("%Y-%m-%d %H:%M:%S"), v[1].strftime("%Y-%m-%d %H:%M:%S")) for v in df.values]
    else:
        return []


def get_entid_by_entname(conn: connect, ent_name: str) -> int:
    sql = f"select id from svc_enterprise where name = %s limit 1;"
    df: pd.DataFrame = __get_yhsdb_query(conn=conn, sql=sql, args=(ent_name,))
    if len(df) == 0:
        return 0
    return df['id'][0]


def get_day_off_list(conn: connect, work_start_firsttime: datetime, work_end_predicttime: datetime, std_worktime_start: str, std_worktime_end: str,
                     ent_id: int, mkey: int) -> list:
    # 1. DB에서 일자범위내의 주말+공휴일을 리스트로 만든다.
    df_holidays: pd.DataFrame = sel_ref_holidays(conn=conn, std_worktime_start=std_worktime_start, std_worktime_end=std_worktime_end,
                                                 ent_id=ent_id, mkey=mkey,
                                                 start_datetime=work_start_firsttime, end_datetime=work_end_predicttime)
    return [(df_day[0], df_day[1]) for df_day in df_holidays.values]


if __name__ == '__main__':
    # print(sel_ref_std_worktime(ent_id=1))
    test_env = dotenv_values()  # .env 환경변수 로드
    test_conn: connect = open_yhsdb_conn(user=test_env["YHS_DB_CYCLEDATA_USER"],
                                         pw=test_env["YHS_DB_CYCLEDATA_PW"],
                                         host=test_env["YHS_DB_CYCLEDATA_HOST"],
                                         database=test_env["YHS_DB_CYCLEDATA_DB"],
                                         charset="utf8")
    print(sel_process_hist(conn=test_conn, lot=29626))
