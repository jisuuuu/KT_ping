# Project Name : KT 해저케이블
# OS           : Windows / Linux
# DATABASE     : MySQL
# PROGRAM NAME : KT_ping
# COMPILER     : Python3.6.2 64x
# DESCRIPTION  : Tcpping 및 ping 엔진
# AUTHER       : Choijh
# 프로그램 실행 인자 1로 jdbc_maria.properties 파일의 위치를 인자로 전달한다.

import sql_query
import sys
import datetime
import subprocess
import queue
import threading
import http.client
import socket
import pymysql
import logger
from icmpping import ping
import os
import configLoader
import hashlib

ENG_NAME = 'KT_ping'

if os.name == 'nt':
    FULL_PATH = configLoader.GetConfValueStr(ENG_NAME, "FULL_PATH", "C:\\netis\\bin\\")
else:
    FULL_PATH = configLoader.GetConfValueStr(ENG_NAME, "FULL_PATH", "/home/netis/bin/")

yyyymmdd = "{:%Y%m%d}".format(datetime.datetime.now())
hhmmss = "{:%H%M%S}".format(datetime.datetime.now())

lock = threading.Lock()

NETIS_ROOT = os.getenv('NETIS_ROOT')
if NETIS_ROOT is None:
    if os.name == 'nt':
        NETIS_ROOT = "C:\\netis"
    else:
        NETIS_ROOT = '/home/netis'

DEFAUTL_BIN_PATH = os.path.join(NETIS_ROOT, 'bin')

g_ndebug = configLoader.GetConfValueInt(ENG_NAME, "DEBUG", 1)

mutex = threading.Lock()


def sender_data_end(url, port):
    conn = http.client.HTTPSConnection('%s:%s' % (url, port), timeout=1)
    payload = """ {
                    "type":"publish",
                    "address":"js.EndLine",
                    "body":{
                        }
                    }"""
    log.log.debug(payload)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((url, int(port)))
        s.send(payload.encode('utf-8'))
        s.close()
        # conn.request("POST", "/", payload.encode("utf-8"))
    except socket.timeout:
        return False

    return True


def sender_internet(url, port, data):
    conn = http.client.HTTPSConnection('%s:%s' % (url, port), timeout=1)
    payload = """ {
                    "type":"publish",
                    "address":"js.InternetLine",
                    "body":{
                        "crtDt":"%s",
                        "successCnt":%s,
                        "lineRespMin":"%sms",
                        "lineRespAvg":"%sms",
                        "lineRespMax":"%sms",
                        "status":%s,
                        "userChk":"%s",
                        "internetLine":%s,
                        "lineNo":%s,
                        "srcLineIp":"%s"
                        }
                    }""" % (data[0], data[1], data[2], data[3], data[4], data[5],
                            data[6], data[7], data[8], data[9])
    log.log.debug("Internet Line Json Send %s" % payload)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((url, int(port)))
        s.send(payload.encode('utf-8'))
        s.close()
        # conn.request("POST", "/", payload.encode("utf-8"))
    except socket.timeout:
        return False

    return True


def sender_data(url, port, data):
    conn = http.client.HTTPSConnection('%s:%s' % (url, port), timeout=1)
    payload = """ {
                    "type":"publish",
                    "address":"js.dataLine",
                    "body":{
                        "crtDt":"%s",
                        "successCnt":%s,
                        "lineRespMin":"%sms",
                        "lineRespAvg":"%sms",
                        "lineRespMax":"%sms",
                        "status":%s,
                        "userChk":"%s",
                        "lineNo":%s,
                        "lineIp":"%s"
                        }
                    }""" % (data[0], data[1], data[2], data[3], data[4], data[5],
                            data[6], data[8], data[9])
    log.log.debug("DataLines Json Send %s" % payload)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((url, int(port)))
        s.send(payload.encode('utf-8'))
        s.close()
        # conn.request("POST", "/", payload.encode("utf-8"))
    except socket.timeout:
        return False

    return True


def sender_evt(url, port, data):
    conn = http.client.HTTPSConnection('%s:%s' % (url, port), timeout=1)
    payload = """ {
                    "type":"publish",
                    "address":"js.dataLine",
                    "body":{
                        "crtDt":"%s",
                        "successCnt":%s,
                        "lineRespMin":"%sms",
                        "lineRespAvg":"%sms",
                        "lineRespMax":"%sms",
                        "status":%s,
                        "userChk":"%s",
                        "lineIp":"%s"
                        }
                    }""" % (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[9])
    log.log.debug("Event Json Send %s" % payload)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((url, int(port)))
        s.send(payload.encode('utf-8'))
        s.close()
        # conn.request("POST", "/", payload.encode("utf-8"))
    except socket.timeout:
        return False

    return True


class KTcableDBjob():
    def __init__(self):  # , data_result_queue, internet_result_queue):
        self.end_of_data = True

    def __del__(self):
        if self.curs is not None:
            self.curs.close()
            self.connection.close()

    def connect_by_config(self):
        src_db_dbip = configLoader.GetConfValueStr("KT_ping", "DB_IP", "12324")
        src_db_dbport = configLoader.GetConfValueInt("KT_ping", "DB_PORT", 1234)
        src_db_user = configLoader.GetConfValueStr("KT_ping", "DB_ID", "nexis")
        src_db_pwd = configLoader.GetConfValueStr("KT_ping", "DB_PWD", "1234")
        src_db_name = configLoader.GetConfValueStr("KT_ping", "DB_SID", "dfdfdf")
        try:
            self.connection = pymysql.connect(host=src_db_dbip, port=src_db_dbport, user=src_db_user,
                                              password=src_db_pwd, db=src_db_name, charset='utf8')
            self.curs = self.connection.cursor()
        except pymysql.MySQLError as e:
            log.log.error("MySQL Error in Connection [%s] [%s]" % (e.args[0], e.args[1]))

        self.config = self.get_config()

    def get_config(self):
        rtn_map = {}
        try:
            self.curs.execute("SELECT CD_ID, CD_VAL1 FROM TB_CFG_CODE WHERE CD_TYPE = 'ENGINE_SETTING'")
            data = self.curs.fetchall()
        except pymysql.MySQLError as e:
            log.log.error("MySQL Error in get_config [%s] [%s]" % (e.args[0], e.args[1]))

        for idx in data:
            rtn_map[idx[0]] = idx[1]

        return rtn_map

    def get_total_ip_list(self, std_num, proc_num):
        func_name = f'{sys._getframe().f_code.co_name}'

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # 전체 % std_num = proc_num 인 total ip list
        try:
            with lock:
                self.curs.execute(sql_query.TOTAL_LIST.format(std_num, proc_num))
                result = self.curs.fetchall()

                data = []
                for rs in result:
                    data.append(dict(zip([d[0] for d in self.curs.description], rs)))
        except pymysql.MySQLError as e:
            log.log.error("MySQL Error in get_total_ip_list [%s] [%s]" % (e.args[0], e.args[1]))
            return
        return data

    def all_update_data(self, data, value):
        func_name = f'{sys._getframe().f_code.co_name}'

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # 직전 평균 값
        try:
            with lock:
                self.curs.execute(sql_query.DATA_LAST_AVG.format(data[9], data[8]))
                last_data = self.curs.fetchone()

                if last_data is not None:
                    last_data = float(last_data[0].replace("ms", ""))
                else:
                    last_data = 0.0
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # Data ping result insert
        try:
            with lock:
                insert_sql = sql_query.DATA_INSERT.format(data[0], data[1], data[2], data[3], data[4], data[5], data[6],
                                                          data[8], data[9])
                self.curs.execute(insert_sql)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # Data ping result update
        try:
            with lock:
                update_sql = sql_query.DATA_UPDATE.format(data[1], data[2], data[3], data[4], data[5], data[6], data[8],
                                                          data[9])
                self.curs.execute(update_sql)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # 직전 평균 값과 지정한 값과 비교 후 차이가 크면 vertix 전송
        if abs(last_data - data[3]) >= value:
            sender_data(self.config["POST_IP"], self.config["POST_PORT"], data)

    def all_update_internet(self, data, value):
        func_name = f'{sys._getframe().f_code.co_name}'

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # 직전 평균 값
        try:
            with lock:
                self.curs.execute(sql_query.INTERNET_LAST_AVG.format(data[9], data[8]))
                last_data = self.curs.fetchone()

                if last_data is not None:
                    last_data = float(last_data[0].replace("ms", ""))
                else:
                    last_data = 0.0
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # Internet ping result insert
        try:
            with lock:
                insert_sql = sql_query.INTERNET_INSERT.format(data[1], data[2], data[3], data[4], data[5], data[6], data[0],
                                                              data[8], data[7], data[9])
                self.curs.execute(insert_sql)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # Internet ping result update
        try:
            with lock:
                update_sql = sql_query.INTERNET_UPDATE.format(data[1], data[2], data[3], data[4], data[5], data[6], data[9])
                self.curs.execute(update_sql)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # 직전 평균 값과 지정한 값과 비교 후 차이가 크면 vertix 전송
        if abs(last_data - data[3]) >= value:
            sender_internet(self.config["POST_IP"], self.config["POST_PORT"], data)

    def all_update_evt(self, data, value):
        func_name = f'{sys._getframe().f_code.co_name}'

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # 직전 평균 값
        try:
            with lock:
                self.curs.execute(sql_query.EVT_LAST_AVG.format(data[9], data[8]))
                last_data = self.curs.fetchone()

                if last_data is not None:
                    last_data = float(last_data[0].replace("ms", ""))
                else:
                    last_data = 0.0
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # row num 초기화
        try:
            with lock:
                self.curs.execute(sql_query.SETTING)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # Event ping result update
        try:
            with lock:
                update_sql = sql_query.EVT_UPDATE.format(data[1], data[2], data[3], data[4], data[5], data[6], data[9])
                self.curs.execute(update_sql)
                self.connection.commit()
        except Exception as e:
            log.log.error(f'{func_name} {type(e)} {e}')

        # 직전 평균 값과 지정한 값과 비교 후 차이가 크면 vertix 전송
        if abs(last_data - data[3]) >= value:
            sender_evt(self.config["POST_IP"], self.config["POST_PORT"], data)


class Worker(threading.Thread):
    def __init__(self, job_queue, result_queue, total_ip, ping_interval, ping_count, ping_timeout):
        threading.Thread.__init__(self)
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.total_ip = total_ip
        self.ping_interval = ping_interval
        self.ping_count = ping_count
        self.ping_timeout = ping_timeout

    def run(self):
        while self.job_queue.empty() is not True:
            # Ping 작업
            target = self.job_queue.get()
            if target['TYPE'] == 'D':
                result = ping(target['LINE_IP'], self.ping_timeout, self.ping_count)
                log.log.info(f"Data Lines Ping {target['LINE_IP']} : success[{result[0]}], min result[{result[1]}], "
                             f"max result[{result[2]}], average result[{result[3]}]")

            elif target['TYPE'] == 'I':
                result = ping(target['LINE_IP'], self.ping_timeout, self.ping_count)
                log.log.info(f"Internet Line Ping {target['LINE_IP']} : success[{result[0]}], min result[{result[1]}], "
                             f"max result[{result[2]}], average result[{result[3]}]")

            elif target['TYPE'] == 'E':
                result = ping(target['LINE_IP'], self.ping_timeout, self.ping_count)
                log.log.info(f"Event Line Ping {target['LINE_IP']} : success[{result[0]}], min result[{result[1]}], "
                             f"max result[{result[2]}], average result[{result[3]}]")

            # Total ip list 와 distinct ip list 매핑 작업
            for ip in self.total_ip:
                if ip['LINE_IP'] == target['LINE_IP']:
                    real_result = [datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
                                   result[0], result[1] / 2 if result != 0 else 0, result[3] / 2 if result != 0 else 0,
                                   result[2] / 2 if result != 0 else 0, 1 if result[0] != 0 else 0, "N",
                                   ip['INTERNET_LINE_NO'], ip['LINE_NO'], target['LINE_IP']]
                    self.result_queue.put(real_result)


class KTping:
    def __init__(self):
        self.db = KTcableDBjob()
        self.refresh_config()

    def refresh_config(self):
        self.db.connect_by_config()
        self.config = self.db.get_config()
        self.ping_interval = float(self.config['PING_INTERVAL'])
        self.ping_count = int(self.config['PING_COUNT'])
        self.ping_timeout = float(self.config['PING_TIMEOUT'])
        self.engine_thread = int(self.config['ENGINE_THREAD'])
        self.difference = int(self.config['DIFFERENCE'])
        self.thread_num = int(self.config['THREAD_NUM'])

    def cycle(self, total_ip_list, proc_num, n):
        # Data, Internet, Event 별로 돌리기 위한 Queue 생성
        internet_target_queue = queue.Queue()
        internet_result_queue = queue.Queue()
        internet_total_ip = []

        data_target_queue = queue.Queue()
        data_result_queue = queue.Queue()
        data_total_ip = []

        evt_target_queue = queue.Queue()
        evt_result_queue = queue.Queue()
        evt_total_ip = []

        # distinct ip list 작업
        ip_list = []
        data = list({d['LINE_IP']: d for d in total_ip_list}.values())
        for d in data:
            ip_list.append(dict({key: value for key, value in d.items() if key == 'LINE_IP' or key == 'TYPE'}))

        log.log.debug(total_ip_list)
        log.log.debug(ip_list)

        # Data, Internet, Event 별로 돌리기 위한 작업
        for ip in total_ip_list:
            if ip['TYPE'] == 'I':
                internet_total_ip.append(ip)
            elif ip['TYPE'] == 'D':
                data_total_ip.append(ip)
            elif ip['TYPE'] == 'E':
                evt_total_ip.append(ip)

        for ip in ip_list:
            if ip['TYPE'] == 'I':
                internet_target_queue.put(ip)
            elif ip['TYPE'] == 'D':
                data_target_queue.put(ip)
            elif ip['TYPE'] == 'E':
                evt_target_queue.put(ip)

        log.log.info(f"[Thread{proc_num}_{n}] Start")
        thread_lst = {}
        # 각각 engine thread 개수만큼 실행
        for idx in range(self.engine_thread):
            thread_lst['DATA_%s' % idx] = Worker(data_target_queue, data_result_queue, data_total_ip,
                                                 self.ping_interval, self.ping_count, self.ping_timeout)
            thread_lst['DATA_%s' % idx].start()

            thread_lst['INTERNET_%s' % idx] = Worker(internet_target_queue, internet_result_queue,
                                                     internet_total_ip, self.ping_interval, self.ping_count,
                                                     self.ping_timeout)
            thread_lst['INTERNET_%s' % idx].start()

            thread_lst['EVT_%s' % idx] = Worker(evt_target_queue, evt_result_queue, evt_total_ip,
                                                self.ping_interval, self.ping_count, self.ping_timeout)
            thread_lst['EVT_%s' % idx].start()

        # 종료 확인
        for idx_key in thread_lst.keys():
            thread_lst[idx_key].join()

        log.log.info(f"[Thread{proc_num}_{n}] End")

        # Data, Internet, Event 별 db 작업
        log.log.info(f"[Thread{proc_num}_{n}] DB Job Start")
        while not data_result_queue.empty():
            self.db.all_update_data(data_result_queue.get(), self.difference)

        while not internet_result_queue.empty():
            self.db.all_update_internet(internet_result_queue.get(), self.difference)

        while not evt_result_queue.empty():
            self.db.all_update_evt(evt_result_queue.get(), self.difference)
        log.log.info(f"[Thread{proc_num}_{n}] DB Job End")

        # time.sleep(3)
        # if sender_data_end(self.db.config["POST_IP"], self.db.config["POST_PORT"]) is False:
        #     log.log.error("POST Data Send Fail")
        # else:
        #     log.log.info("POST Data Send Ended")


def main_worker(std_num, proc_num):
    func_name = f'{sys._getframe().f_code.co_name}'

    # KT Ping 생성 및 db 연결
    try:
        kt_ping = KTping()
        kt_ping.db.connect_by_config()
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db connect error')
        sys.exit(1)

    # 전체 row num 중에 std_num으로 나눴을 때 proc_num 이 나오는 리스트 목록
    try:
        total_ip_list = kt_ping.db.get_total_ip_list(std_num, proc_num)
        log.log.info(f'[process_{proc_num}] total_ip_list : {total_ip_list}')
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db execute error')

    # main worker 에서 돌릴 thread 갯수
    n = kt_ping.thread_num
    log.log.info(f'[process_{proc_num}] thread num : {n}')

    # thread 별로 돌리기 위한 Hash 를 사용한 분배 작업
    real_ip_list = dict()

    for i in range(0, n):
        real_ip_list[i] = []

    for ip in total_ip_list:
        enc = hashlib.sha256(ip['LINE_IP'].encode('utf-8'))
        encText = enc.hexdigest()
        log.log.debug(f"[process_{proc_num}] ip : {ip['LINE_IP']} Hash : '{encText}")
        real_ip_list[int(encText, 16) % n].append(ip)

    new = list(real_ip_list.values())

    # process 별 thread 시작
    log.log.info(f'[process_{proc_num}] Start')
    threads = []
    for i in range(0, n):
        thread = threading.Thread(target=kt_ping.cycle, args=(new[i], proc_num, i))
        threads.append(thread)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
    log.log.info(f'[process_{proc_num}] End')


# subprocess 호출
def exec_child_prcs():
    func_name = f'{sys._getframe().f_code.co_name}'

    # KT cable db 연결
    try:
        db = KTcableDBjob()
        db.connect_by_config()
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db connect error')
        sys.exit(1)

    # process 분배를 위한 전체 갯수
    try:
        db.curs.execute(sql_query.TOTAL_COUNT)
        count = int(db.curs.fetchone()[0])
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db execute error')

    # process num
    try:
        db.curs.execute(sql_query.PROCESS_NUM)
        process_num = int(db.curs.fetchone()[0])
        log.log.info(f'process num : {process_num}')
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db execute error')

    # Data, internet, event 망의 갯수
    try:
        db.curs.execute(sql_query.INTERNET_COUNT)
        internet_cnt = db.curs.fetchone()[0]
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db execute error')

    try:
        db.curs.execute(sql_query.DATA_COUNT)
        data_cnt = db.curs.fetchone()[0]
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db execute error')

    try:
        db.curs.execute(sql_query.EVT_COUNT)
        evt_cnt = db.curs.fetchone()[0]
    except Exception as e:
        log.log.error(f'[{func_name}] {e} db execute error')

    log.log.info("GetJobList Count Internet Line[%s], Data Line[%s] Event Line[%s]" % (internet_cnt, data_cnt, evt_cnt))

    procs_list = [i for i in range(0, int(count / process_num))]
    log.log.info(f'Process list : {procs_list}')
    dict_prcs = {}

    # 멀티프로세스 실행
    try:
        for proc in procs_list:
            exec_cmd = f'{os.path.join(DEFAUTL_BIN_PATH, ENG_NAME)} "{int(count / process_num)}" "{proc}"'
            procs = subprocess.Popen(exec_cmd, shell=True)
            dict_prcs[exec_cmd] = procs
    except Exception as e:
        log.log.error(f'{func_name} {type(e)} {e}')


if __name__ == "__main__":
    if len(sys.argv[1:]) == 0:
        log = logger.Logger()
        log.setLogFile(f"{ENG_NAME}_{yyyymmdd}.log")
        log.setLogLevel('INFO')
        log.log.info("####################################   S T A R T   ####################################")
        exec_child_prcs()
        log.log.info("####################################     E N D     ####################################")
    else:  # child process
        log = logger.Logger()
        log.setLogFile(f"{ENG_NAME}_{sys.argv[2]}_{yyyymmdd}.log")
        log.setLogLevel('INFO')
        log.log.info("####################################   S T A R T   ####################################")
        main_worker(int(sys.argv[1]), int(sys.argv[2]))
        log.log.info("####################################     E N D     ####################################")
        sys.exit(0)