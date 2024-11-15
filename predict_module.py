import pandas as pd
import numpy as np
from yhs_common import *
from datetime import datetime, timedelta
from typing import Union


def checking_idle_active(time_list) -> list:
    """
    누적 입력 데이터 확인
     - (대기 시간, 구동 시간) 형태의 리스트 반환
    """
    idle_active_list = []
    if len(time_list) == 1:
        return []
    else:
        for i in range(len(time_list)-1):
            idle_time = (time_list[i+1][0] - time_list[i][1]).seconds
            active_time = (time_list[i+1][1] - time_list[i+1][0]).seconds
            idle_active_list.append((idle_time, active_time))
        return idle_active_list


def idle_active_time(time_list, idle_active_list) -> tuple:
    """
    idle & active time 계산, 단위: 초
     - (sum(idle time), sum(active time)) 의 형태를 반환
    """
    if 2 <= len(time_list) <= 6:
        df_time = pd.DataFrame(idle_active_list, columns=['idle_time', 'active_time'])
        i_time, a_time = round(df_time.idle_time.mean()), round(df_time.active_time.mean())
        # print('평균 계산중 입니다...')
        return i_time, a_time
    else:
        df_time = pd.DataFrame(idle_active_list, columns=['idle_time', 'active_time'])

        Q1, Q3 = np.percentile(df_time['idle_time'], 25), np.percentile(df_time['idle_time'], 75)
        idel_time_IQR_M = df_time[(df_time['idle_time'] >= Q1) & (df_time['idle_time'] <= Q3)]['idle_time'].mean()
        Q1, Q3 = np.percentile(df_time['active_time'], 25), np.percentile(df_time['active_time'], 75)
        active_time_IQR_M = df_time[(df_time['active_time'] >= Q1) & (df_time['active_time'] <= Q3)]['active_time'].mean()

        i_time, a_time = round(idel_time_IQR_M), round(active_time_IQR_M)
        return i_time, a_time


def timetable_checking(now_time, timetable, add_timetable, add_table=False):
    """입력데이터의 시간을 보았을 때 유휴시간 및 추가가공시간의 타임테이블이 올바른지"""
    while True:
        # log()
        if len(timetable) > 0 and len(add_timetable) > 0:
            time_str_s, time_str_e = timetable[0][0], timetable[0][1]
            time_s = datetime.strptime(time_str_s, '%Y-%m-%d %H:%M:%S')
            time_e = datetime.strptime(time_str_e, '%Y-%m-%d %H:%M:%S')

            addtime_str_s, addtime_str_e = add_timetable[0][0], add_timetable[0][1]
            addtime_s = datetime.strptime(addtime_str_s, '%Y-%m-%d %H:%M:%S')
            addtime_e = datetime.strptime(addtime_str_e, '%Y-%m-%d %H:%M:%S')

            if now_time > time_s:
                # break_time에 add_time이 포함되어 있을 경우 (주말의 경우)
                if (time_s <= addtime_s) & (time_e >= addtime_e) & (add_table is False):
                    time_s = addtime_e
                    timetable.pop(0)
                    timetable.insert(0, (str(time_s), str(time_e)))

                # add_time 사이에 now_time이 껴있을 경우, 테이블 변경
                elif (now_time > time_s) & (now_time < time_e) & (add_table is True):
                    time_s = now_time
                    timetable.pop(0)  # 빼고
                    timetable.insert(0, (str(time_s), str(time_e)))  # 맨 앞자리에 추가
                else:
                    timetable.pop(0)
            else:
                return timetable
        else:
            return timetable


def pop_timetable(timetable, break_time=True) -> tuple:
    """타임 테이블에서 맨 앞에 있는 시간대를 뽑아 비교 하기 시작,
       만약 시간대가 없으면 무한대 시간대 적용"""
    if len(timetable) > 0:
        _str = timetable.pop(0)
        _time_s = datetime.strptime(_str[0], "%Y-%m-%d %H:%M:%S")  # Change the type of break start time
        _time_e = datetime.strptime(_str[1], "%Y-%m-%d %H:%M:%S")  # Change the type of break end time
    else:
        if break_time:
            _time_s = datetime(2099, 12, 31, 9, 0, 0)
            _time_e = datetime(2099, 12, 31, 10, 0, 0)
        else:
            _time_s = datetime(2199, 12, 31, 9, 0, 0)
            _time_e = datetime(2199, 12, 31, 10, 0, 0)
    return _time_s, _time_e


def plus_time_step(now_time, time_step):
    """현재시간에 time step 더하기"""
    now_time = now_time + timedelta(seconds=time_step)
    return now_time


def nextday_std_working_s(now_time, working_s) -> datetime:
    """다음날짜의 표준 근로시간을 리턴"""
    change_date = now_time + timedelta(seconds=86400)  # + 1day (24h=86400sec)
    Y, M, D = change_date.year, change_date.month, change_date.day
    h, m, s = working_s.hour, working_s.minute, working_s.second
    
    return datetime(Y, M, D, h, m, s)  # now_time


def working_astype_datetime(now_time, working):
    """날짜 초기화, 타입 변경 시"""
    Y, M, D = now_time.year, now_time.month, now_time.day
    working = working.split(':')
    h, m, s = int(working[0]), int(working[1]), int(working[2])
    return datetime(Y, M, D, h, m, s)


def working_astype_str(working):
    """타입 변경"""
    h, m, s = working.hour, working.minute, working.second
    h, m, s = str(h), str(m), str(s)
    working = f"{h}:{m}:{s}"
    return working


def fix_break(now_time, fix_break_timetable):
    """고정 쉬는시간 전처리"""
    
    plus_fix_break = []
    
    Y = now_time.year
    M = now_time.month
    D = now_time.day
    
    for t in fix_break_timetable:
        fix = t[0].split(':')
        fix2 = t[1].split(':')
        h, m, s = int(fix[0]), int(fix[1]), int(fix[2])
        h2, m2, s2 = int(fix2[0]), int(fix2[1]), int(fix2[2])
        
        fix_breaktime = datetime(Y, M, D, h, m, s)
        fix_breaktime2 = datetime(Y, M, D, h2, m2, s2)
        plus_fix_break.append((fix_breaktime, fix_breaktime2))
    
    return plus_fix_break


#  --------------------------------- Model -----------------------------------------
def predict_program(now_time, now_count, plan: int, batch_size: int,
                    fix_break_timetable, break_timetable, add_process_timetable,
                    idle_time: int, active_time: int, working_s: str, working_e: str) -> datetime:
    # fix value
    time_step = idle_time+active_time
    count = now_count
    
    # process work about fix breaktime
    if len(fix_break_timetable) > 0:
        plus_fix_break = fix_break(now_time, fix_break_timetable)

        plus_fix_break2 = []
        for t in plus_fix_break:
            if now_time <= t[1]:
                fix_s = str(t[0])
                fix_e = str(t[1])
                plus_fix_break2.append((fix_s, fix_e))

        if len(plus_fix_break2) > 0:
            for t in plus_fix_break2:
                break_timetable.append(t)
            break_timetable.sort()
            
    # process work about timetable
    if len(break_timetable) == 0:
        break_timetable.append(('2099-12-31 09:00:00', '2099-12-31 18:00:00'))
    if len(add_process_timetable) == 0:
        add_process_timetable.append(('2199-12-31 09:00:00', '2199-12-31 18:00:00'))
    
    # ===================== program ====================== #
    
    # process work about breaktime and Additional processing time
    break_time_s, break_time_e = pop_timetable(break_timetable)
    add_process_s, add_process_e = pop_timetable(add_process_timetable)
    
    # it is first?
    first = True
    
    # for additional process
    add_process_imp = []
    add = False
    working_e_fix = working_astype_datetime(now_time, working_e)
    
    # iteration
    while plan > count:
        # log(f"{plan} / {count} / {batch_size}")
        if first:
            working_s = working_astype_datetime(now_time, working_s)
            working_e = working_astype_datetime(now_time, working_e)
            first = False
        
        # now_time의 year, month, day에 대하여 초기화
        else:
            working_s = working_astype_str(working_s)  # astype(str)
            working_e = working_astype_str(working_e)  # astype(str)
            working_s = working_astype_datetime(now_time, working_s)  # astype(datetime)
            working_e = working_astype_datetime(now_time, working_e)  # astype(datetime)

        # *** 유휴시간 타임테이블 조정 ***# ----- 수정(2)
        # break_time 및 add_time의 종료시간(e_time)이 now_time보다 이전으로 있다면 제거.
        while now_time > break_time_e:
            # log()
            break_time_s, break_time_e = pop_timetable(break_timetable)

        # add_process_time을 다 무한대로 보내버릴 수도 있음...
        # ----- 수정(3)(4)
        while now_time > add_process_e:
            # log()
            if (working_s.month == add_process_s.month) & (working_s.day == add_process_s.day) & (now_time >= add_process_e):
                now_time = add_process_s
                add_process_s, add_process_e = pop_timetable(add_process_timetable)
                count -= batch_size
            else:
                add_process_s, add_process_e = pop_timetable(add_process_timetable)

        now_t__break_t__second = int((break_time_s - now_time).total_seconds())

        # 추가가공시간 발생 시
        if now_t__break_t__second <= time_step:
            if (break_time_s <= add_process_s) & (add_process_e <= break_time_e):
                
                # case3 : (표준근로시간안에 or 유휴시간안에) 추가가공시간이 발생했을 때
                if break_time_s == add_process_s:  # case3-1
                    break_time_s = add_process_e
                    now_t__break_t__second = int((break_time_s - now_time).total_seconds())
                    
                elif break_time_e == add_process_e:  # case3-3
                    break_time_e = add_process_s

                # case3-2
                else:
                    # 유휴시간 분리 및 생성
                    new_break_time = (str(add_process_e), str(break_time_e))
                    break_timetable.insert(0, new_break_time)
                    break_time_e = add_process_s
                
                # 고정 쉬는시간 추가
                if len(fix_break_timetable) > 0:
                    plus_fix_break2 = []
                    break_timetable.append((str(break_time_s), str(break_time_e)))
                    plus_fix_break = fix_break(now_time, fix_break_timetable)
                    for t in plus_fix_break:
                        fix_s = str(t[0])
                        fix_e = str(t[1])
                        plus_fix_break2.append((fix_s, fix_e))

                    for t in plus_fix_break2:
                        break_timetable.append(t)
                    break_timetable.sort()
                    break_time_s, break_time_e = pop_timetable(break_timetable)
                
        # 유휴시간 발생 시
        if now_t__break_t__second <= time_step:
            
            # case1: 유휴시간이 가공시간에 발생했을 때
            if now_t__break_t__second >= idle_time:
                now_time = break_time_e
                count += batch_size
              
            # case2: 유휴시간이 센업시간에 발생했을 때
            else:
                now_time = plus_time_step(now_time, time_step)
                new_more_time = break_time_e - break_time_s
                now_time += new_more_time
                count += batch_size
                
            break_time_s, break_time_e = pop_timetable(break_timetable)
        
        # *** + time_step(s) ***
        else:
            now_time = plus_time_step(now_time, time_step)
            count += batch_size
            
        # replace date
        # case4: 표준근로시간밖에서 추가가공시간이 발생했을 때
        if now_time >= working_e:  # 근무시간을 초과할 때
            new_time = nextday_std_working_s(now_time, working_s)

            if (working_e <= add_process_s) & (add_process_e <= new_time) & (len(add_process_imp) == 0):
                add_process_timetable.insert(0, (str(add_process_s), str(add_process_e)))
                
                for i in range(len(add_process_timetable)):
                    add_process_str = add_process_timetable[i]
                    add_process_s = datetime.strptime(add_process_str[0], "%Y-%m-%d %H:%M:%S")
                    add_process_e = datetime.strptime(add_process_str[1], "%Y-%m-%d %H:%M:%S")
                    if (working_e <= add_process_s) & (add_process_e <= new_time):
                        add_process_imp.append(add_process_str)

            if len(add_process_imp) != 0:
                add_process_timetable.pop(0)
                add_process_str = add_process_imp.pop(0)
                add_process_s = datetime.strptime(add_process_str[0], "%Y-%m-%d %H:%M:%S")
                add_process_e = datetime.strptime(add_process_str[1], "%Y-%m-%d %H:%M:%S")
                
                now_time = add_process_s  # 초기화
                working_e = add_process_e  # 초기화

                # 예외처리--수정(2)
                if (working_s.hour, working_s.minute, working_s.second) == (working_e.hour, working_e.minute, working_e.second):
                    working_e = working_e_fix
                    # --수정(3)
                    if len(fix_break_timetable) > 0:
                        plus_fix_break2 = []
                        break_timetable.append((str(break_time_s), str(break_time_e)))
                        plus_fix_break = fix_break(now_time, fix_break_timetable)
                        for t in plus_fix_break:
                            fix_s = str(t[0])
                            fix_e = str(t[1])
                            plus_fix_break2.append((fix_s, fix_e))

                        for t in plus_fix_break2:
                            break_timetable.append(t)
                        break_timetable.sort()
                        break_time_s, break_time_e = pop_timetable(break_timetable)

                    # --수정(3)
                    # ex) 표준근로시작 시간 07:00:00 이고, 추가가공 시간: 3-21 06:40:00, 3-21 07:00:00 이라면
                    # 단지, 표준근로 시작 시간이 06:40:00으로 당겨진 것이기 때문에 add_process_time을 하나 빼준다.
                    add_process_s, add_process_e = pop_timetable(add_process_timetable, break_time=False)

                count -= batch_size

                if len(add_process_imp) == 0:
                    add = True
                    continue
                continue
                
            if add is True:
                add_process_s, add_process_e = pop_timetable(add_process_timetable, break_time=False)

                if new_time < add_process_s:
                    now_time = new_time  # working_s_fix
                    working_e = working_e_fix

                count -= batch_size
                add = False
                
                # 고정 쉬는시간 추가
                if len(fix_break_timetable) > 0:
                    plus_fix_break2 = []
                    break_timetable.append((str(break_time_s), str(break_time_e)))
                    plus_fix_break = fix_break(now_time, fix_break_timetable)
                    for t in plus_fix_break:
                        fix_s = str(t[0])
                        fix_e = str(t[1])
                        plus_fix_break2.append((fix_s, fix_e))

                    for t in plus_fix_break2:
                        break_timetable.append(t)
                    break_timetable.sort()
                    break_time_s, break_time_e = pop_timetable(break_timetable)
                
            else:
                now_time = new_time
                count -= batch_size
                
                # 고정 쉬는시간 추가
                if len(fix_break_timetable) > 0:
                    plus_fix_break2 = []
                    break_timetable.append((str(break_time_s), str(break_time_e)))
                    plus_fix_break = fix_break(now_time, fix_break_timetable)

                    for t in plus_fix_break:
                        fix_s = str(t[0])
                        fix_e = str(t[1])
                        plus_fix_break2.append((fix_s, fix_e))

                    for t in plus_fix_break2:
                        break_timetable.append(t)

                    break_timetable.sort()
                    break_time_s, break_time_e = pop_timetable(break_timetable)

    # -- end of while -- #

    # 결과 확인하려면 ctrl+/ 를 눌러주세요.   
    # print(f"\n\n\n Predict completed time: {now_time} / count: {count} / run time: {time.time()-start}(s) \n\n\n")
    return now_time


def get_predict_cmplt_time(df: pd.DataFrame, table_fix_brk_time: list, table_brk_time: list, table_add_work_time: list,
                           working_s: str, working_e: str, plan: int, batch_size: int) -> Union[datetime, None]:
    time_list = []

    # df['start'], df['end'] = pd.to_datetime(df['start']), pd.to_datetime(df['end'])  # 어차피 db에서 timestamp 타입으로 가져오기 때문에 불필요

    for i in range(len(df)):
        time_list.append((df['start'][i], df['end'][i]))

    # (대기시간, 구동시간) 형태의 리스트를 반환. 전체rows - 1 의 개수를 가진다.
    idle_active_list = checking_idle_active(time_list)

    # 누적 입력 데이터 1개 이상시 처리
    if len(idle_active_list) > 0:
        # (sum(idle time), sum(active time)) 의 형태를 반환
        idle_time, active_time = idle_active_time(time_list, idle_active_list)

        # 테이블셋의 마지막 데이터 now_time = last_time, now_count = last_count
        now_time, now_count = df['end'].iloc[-1], df['count'].iloc[-1]

        table_fix_brk_time_copy = table_fix_brk_time.copy()
        table_brk_time_copy = table_brk_time.copy()
        table_add_work_time_copy = table_add_work_time.copy()

        add_process_timetable = timetable_checking(now_time, table_add_work_time_copy, table_add_work_time_copy, True)
        break_timetable = timetable_checking(now_time, table_brk_time_copy, add_process_timetable, False)
        predict_completed_time = predict_program(now_time=now_time,
                                                 now_count=now_count,
                                                 plan=plan,
                                                 batch_size=batch_size,
                                                 fix_break_timetable=table_fix_brk_time_copy,
                                                 break_timetable=break_timetable,
                                                 add_process_timetable=add_process_timetable,
                                                 idle_time=idle_time,
                                                 active_time=active_time,
                                                 working_s=working_s,
                                                 working_e=working_e)
        return predict_completed_time
    # 누적 입력 데이터 N개
    else:
        print('처리 할 만큼의 충분한 데이터가 없음')
        return None
