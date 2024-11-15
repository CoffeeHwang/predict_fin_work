from datetime import timedelta
from yhs_common import *
import predict_module as predict
from pymysql import connect
from dotenv import dotenv_values
import yhs_database as yhsdb
from typing import Union


# 예측해야 하는 한계일수
MUST_PREDICT_DAY_LIMIT = 365

def get_predict_endtime(request) -> str:

    # request check
    request_json = request.get_json()
    lot = request_json.get("lot")
    if lot is None or lot <= 0:
        return ""

    # db connect
    env = dotenv_values()  # .env 환경변수 로드
    log(f"env values = {env}")
    conn: connect = yhsdb.open_yhsdb_conn(user=env["YHS_DB_CYCLEDATA_USER"],
                                          pw=env["YHS_DB_CYCLEDATA_PW"],
                                          # host=env["YHS_DB_CYCLEDATA_HOST"],
                                          unix_socket=env["YHS_DB_CYCLEDATA_UNIXSOCKET"], # cloud functions 배포시 host 대신 unix_socket 사용하여 접속
                                          database=env["YHS_DB_CYCLEDATA_DB"],
                                          charset="utf8")

    # lot에 해당되는 DB를 읽어온다.
    df = yhsdb.sel_process_hist(conn=conn, lot=lot)

    # df check
    if len(df) <= 1:
        yhsdb.close_yhsdb_conn(conn=conn)
        return ""

    # enterprise id check
    ent_name = df['ent'][0]
    ent_id = yhsdb.get_entid_by_entname(conn=conn, ent_name=ent_name)
    if ent_id == 0:
        yhsdb.close_yhsdb_conn(conn=conn)
        return ""

    # 1. 표준근로 시작/종료시간 세팅
    std_worktime_start, std_worktime_end = yhsdb.sel_ref_std_worktime(conn=conn, ent_id=ent_id)
    if std_worktime_start is None or std_worktime_end is None:
        yhsdb.close_yhsdb_conn(conn=conn)
        return ""

    # 2. 고정 휴식시간 세팅
    table_fix_brk_time: list = yhsdb.sel_ref_brk_time(conn=conn, ent_id=ent_id)

    # 3. 추가 유휴시간 세팅 (주말+공휴일, 사용자 입력값이 리스트로 반환된다.)
    mkey = df["mkey"][0]
    work_start_firsttime = df["start"][0]
    work_end_predicttime = work_start_firsttime + timedelta(days=MUST_PREDICT_DAY_LIMIT)  # 예측일자의 최대범위(일) : 테스트시 30일로 잡고 실제로는 365일(1년) 잡아야 할듯.
    table_add_brk_time: list = yhsdb.get_day_off_list(conn=conn,
                                                      ent_id=ent_id,
                                                      mkey=mkey,
                                                      work_start_firsttime=work_start_firsttime,
                                                      work_end_predicttime=work_end_predicttime,
                                                      std_worktime_start=std_worktime_start,
                                                      std_worktime_end=std_worktime_end)

    # 4. 추가 근로시간 세팅
    table_add_work_time: list = yhsdb.sel_ref_add_worktime(conn=conn,
                                                           ent_id=ent_id,
                                                           mkey=mkey,
                                                           work_start_firsttime=work_start_firsttime,
                                                           work_end_predicttime=work_end_predicttime
                                                           )

    if std_worktime_start is not None and std_worktime_end is not None:

        # 최종행의 plan
        plan = df.iloc[-1]['plan']

        # batch_size = 최종행카운트 - 직전행카운트
        batch_size = df.iloc[-1]['count'] - df.iloc[-2]['count']
        if batch_size <= 0:
            batch_size = 1

        # 첫행부터 i번째 열( df.iloc[:i+1] ) 범위까지만 탐색하여 작업완료예측시간을 리턴 받음
        predict_cmplt_time: datetime = predict.get_predict_cmplt_time(df=df,
                                                                      table_fix_brk_time=table_fix_brk_time,
                                                                      table_brk_time=table_add_brk_time,
                                                                      table_add_work_time=table_add_work_time,
                                                                      working_s=std_worktime_start,
                                                                      working_e=std_worktime_end,
                                                                      plan=plan,
                                                                      batch_size=batch_size)
        # 작업완료 예측시간 DB의 최종행에 업데이트
        yhsdb.set_predict_end_on_process_hist(conn=conn, seq=df.iloc[-1]['seq'], lot=df.iloc[-1]['lot'], predict_end_time=predict_cmplt_time)

        yhsdb.commit(conn=conn)
        yhsdb.close_yhsdb_conn(conn=conn)

        # 결과 로그
        days = ['월', '화', '수', '목', '금', '토', '일']
        log(f"row={len(df)} seq={df.iloc[-1]['seq']} mkey={df.iloc[-1]['mkey']} lot={df.iloc[-1]['lot']} {df.iloc[-1]['plan']}/{df.iloc[-1]['count']} | "
            f"{df.iloc[-1]['start']} ~ {df.iloc[-1]['end']} "
            f"({days[df.iloc[-1]['end'].weekday()]}) "
            f"| {predict_cmplt_time} "
            f"({days[predict_cmplt_time.weekday()] if predict_cmplt_time is not None else None}) "
            )

        return predict_cmplt_time.strftime('%m-%d %H:%M')

    else:
        yhsdb.close_yhsdb_conn(conn=conn)
        return ""
