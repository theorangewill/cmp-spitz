#!/usr/bin/env python

# The MIT License (MIT)
#
# Copyright (c) 2015 Caian Benedicto <caian@ggaunicamp.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

from libspitz import JobBinary, SimpleEndpoint
from libspitz import messaging, config
from libspitz import memstat
from libspitz import make_uid
from libspitz import log_lines

from libspitz import PerfModule

import Args
import sys, threading, os, time, logging, struct, traceback

# Global configuration parameters
jm_killtms = None # Kill task managers after execution
jm_log_file = None # Output file for logging
jm_verbosity = None # Verbosity level for logging
jm_heart_timeout = None # Timeout for heartbeat response
jm_conn_timeout = None # Socket connect timeout
jm_recv_timeout = None # Socket receive timeout
jm_send_timeout = None # Socket send timeout
jm_send_backoff = None # Job Manager delay between sending tasks
jm_recv_backoff = None # Job Manager delay between sending tasks
jm_memstat = None # 1 to display memory statistics
jm_profiling = None # 1 to enable profiling
jm_perf_rinterv = None # Profiling report interval (seconds)
jm_perf_subsamp = None # Number of samples collected between report intervals
jm_heartbeat_interval = None
jm_jobid = None

###############################################################################
# Parse global configuration
###############################################################################
def parse_global_config(argdict):
    global jm_killtms, jm_log_file, jm_verbosity, jm_heart_timeout, \
        jm_conn_timeout, jm_recv_timeout, jm_send_timeout, jm_send_backoff, \
        jm_recv_backoff, jm_memstat, jm_profiling, jm_perf_rinterv, \
        jm_perf_subsamp, jm_heartbeat_interval, jm_jobid

    def as_int(v):
        if v == None:
            return None
        return int(v)

    def as_float(v):
        if v == None:
            return None
        return int(v)

    def as_bool(v):
        if v == None:
            return None
        return bool(v)

    jm_killtms = as_bool(argdict.get('killtms', True))
    jm_log_file = argdict.get('log', None)
    jm_verbosity = as_int(argdict.get('verbose', logging.INFO // 10)) * 10
    jm_heart_timeout = as_float(argdict.get('htimeout', config.heart_timeout))
    jm_conn_timeout = as_float(argdict.get('ctimeout', config.conn_timeout))
    jm_recv_timeout = as_float(argdict.get('rtimeout', config.recv_timeout))
    jm_send_timeout = as_float(argdict.get('stimeout', config.send_timeout))
    jm_recv_backoff = as_float(argdict.get('rbackoff', config.recv_backoff))
    jm_send_backoff = as_float(argdict.get('sbackoff', config.send_backoff))
    jm_memstat = as_int(argdict.get('memstat', 0))
    jm_profiling = as_int(argdict.get('profiling', 0))
    jm_perf_rinterv = as_int(argdict.get('rinterv', 60))
    jm_perf_subsamp = as_int(argdict.get('subsamp', 12))
    jm_heartbeat_interval = as_float(argdict.get('heartbeat-interval', 10))
    jm_jobid = argdict.get('jobid', '')

###############################################################################
# Configure the log output format
###############################################################################
def setup_log():
    root = logging.getLogger()
    root.setLevel(jm_verbosity)
    root.handlers = []
    if jm_log_file == None:
        ch = logging.StreamHandler(sys.stderr)
    else:
        ch = logging.StreamHandler(open(jm_log_file, 'wt'))
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - '+
        '%(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

###############################################################################
# Abort the aplication with message
###############################################################################
def abort(error):
    logging.critical(error)
    exit(1)

###############################################################################
# Parse the definition of a proxy
###############################################################################
def parse_proxy(cmd):
    cmd = cmd.split()

    if len(cmd) != 3:
        raise Exception()

    logging.debug('Proxy %s.' % (cmd[1]))

    name = cmd[1]
    gate = cmd[2].split(':')
    prot = gate[0]
    addr = gate[1]
    port = int(gate[2])

    return (name, { 'protocol' : prot, 'address' : addr, 'port' : port })

###############################################################################
# Parse the definition of a compute node
###############################################################################
def parse_node(cmd, proxies):
    cmd = cmd.split()

    if len(cmd) < 2:
        raise Exception()

    logging.debug('Node %s.' % (cmd[1]))

    name = cmd[1]
    host = name.split(':')
    addr = host[0]
    port = int(host[1])

    # Simple endpoint
    if len(cmd) == 2:
        return (name, SimpleEndpoint(addr, port))

    # Endpoint behind a proxy
    elif len(cmd) == 4:
        if cmd[2] != 'through':
            raise Exception()

        proxy = proxies.get(cmd[3], None)
        if proxy == None:
            raise Exception()

        # Proxies are not supported yet...
        logging.info('Node %s is behind a proxy and will be ignored.' %
            (cmd[1]))
        return None

    # Unknow command format
    raise Exception()

###############################################################################
# Load the list of task managers from a file
###############################################################################
def load_tm_list_from_file(filename = None):
    # Override the filename if it is empty
    if filename == None:
        nodefile = 'nodes.txt'
        filename = os.path.join('.', nodefile)

    logging.debug('Loading task manager list from %s...' % (filename,))

    # Read all lines
    try:
        with open(filename, 'rt') as file:
            lines = file.readlines()
    except:
        logging.warning('Error loading the list of task managers from file!')
        return {}

    lproxies = [parse_proxy(x.strip()) for x in lines if x[0:5] == 'proxy']
    proxies = {}

    for p in lproxies:
        if p != None:
            proxies[p[0]] = p[1]

    ltms = [parse_node(x.strip(), proxies) for x in lines if x[0:4] == 'node']
    tms = {}
    for t in ltms:
        if t != None:
            tms[t[0]] = t[1]

    return tms

###############################################################################
# Load the list of task managers from a file
###############################################################################
def load_tm_list_from_dir(dirname = None):
    # Override the dirname if it is empty
    if dirname == None:
        dirname = 'nodes'

    logging.debug('Loading task manager list from %s...' % (dirname,))

    tms = {}

    # Read all files
    try:
        for f in os.listdir(dirname):
            f = os.path.join(dirname, f)
            if not os.path.isfile(f):
                continue
            tms.update(load_tm_list_from_file(f))
    except:
        logging.warning('Error loading the list of task ' +
            'managers from directory!')
        return {}

    return tms

###############################################################################
# Load the list of task managers from a file
###############################################################################
def load_tm_list():
    tms = load_tm_list_from_file()
    tms.update(load_tm_list_from_dir())
    logging.debug('Loaded %d task managers.' % (len(tms),))
    return tms

###############################################################################
# Exchange messages with an endpoint to begin pushing tasks
###############################################################################
def setup_endpoint_for_pushing(e):
    try:
        # Try to connect to a task manager
        e.Open(jm_conn_timeout)
    except:
        # Problem connecting to the task manager
        # Because this is a connection event,
        # make it a debug rather than a warning
        logging.debug('Error connecting to task manager at %s:%d!',
            e.address, e.port)
        log_lines(traceback.format_exc(), logging.debug)
        e.Close()
        return
    try:
        # Send the job identifier
        e.WriteString(jm_jobid)

        # Ask if it is possible to send tasks
        e.WriteInt64(messaging.msg_send_task)

        # Verify job id of the answer
        jobid = e.ReadString(jm_recv_timeout)

        if jm_jobid != jobid:
            logging.error('Job Id mismatch from %s:%d! Self: %s, task manager: %s!',
                e.address, e.port, jm_jobid, jobid)
            e.Close()
            return False

        # Wait for a response
        response = e.ReadInt64(jm_recv_timeout)

        if response == messaging.msg_send_full:
            # Task mananger is full
            logging.debug('Task manager at %s:%d is full.',
                e.address, e.port)

        elif response == messaging.msg_send_more:
            # Continue to the task pushing loop
            return True

        else:
            # The task manager is not replying as expected
            logging.error('Unknown response from the task manager!')

    except:
        # Problem connecting to the task manager
        logging.warning('Error connecting to task manager at %s:%d!',
            e.address, e.port)
        log_lines(traceback.format_exc(), logging.debug)

    e.Close()
    return False

###############################################################################
# Exchange messages with an endpoint to begin reading results
###############################################################################
def setup_endpoint_for_pulling(e):
    try:
        # Try to connect to a task manager
        e.Open(jm_conn_timeout)
    except:
        # Problem connecting to the task manager
        # Because this is a connection event,
        # make it a debug rather than a warning
        logging.debug('Error connecting to task manager at %s:%d!',
            e.address, e.port)
        log_lines(traceback.format_exc(), logging.debug)
        e.Close()
        return
    try:
        # Send the job identifier
        e.WriteString(jm_jobid)

        # Ask if it is possible to send tasks
        e.WriteInt64(messaging.msg_read_result)

        # Verify job id of the answer
        jobid = e.ReadString(jm_recv_timeout)

        if jm_jobid != jobid:
            logging.error('Job Id mismatch from %s:%d! Self: %s, task manager: %s!',
                e.address, e.port, jm_jobid, jobid)
            e.Close()
            return False

        return True

    except:
        # Problem connecting to the task manager
        logging.warning('Error connecting to task manager at %s:%d!',
            e.address, e.port)
        log_lines(traceback.format_exc(), logging.debug)

    e.Close()
    return False

###############################################################################
# Push tasks while the task manager is not full
###############################################################################
def push_tasks(job, runid, jm, tm, taskid, task, tasklist, completed):
    # Keep pushing until finished or the task manager is full
    sent = []
    while True:
        if task == None:

            # Avoid calling next_task after it's finished
            if completed:
                logging.debug('There are no new tasks to generate.')
                return (True, 0, None, sent)

            # Only get a task if the last one was already sent
            newtaskid = taskid + 1
            r1, newtask, ctx = job.spits_job_manager_next_task(jm, newtaskid)

            # Exit if done
            if r1 == 0:
                return (True, 0, None, sent)

            if newtask == None:
                logging.error('Task %d was not pushed!', newtaskid)
                return (False, taskid, task, sent)

            if ctx != newtaskid:
                logging.error('Context verification failed for task %d!',
                    newtaskid)
                return (False, taskid, task, sent)

            # Add the generated task to the tasklist
            taskid = newtaskid
            task = newtask[0]
            tasklist[taskid] = (0, task)

            logging.debug('Generated task %d with payload size of %d bytes.',
                taskid, len(task) if task != None else 0)

        try:
            logging.debug('Pushing %d...', taskid)

            # Push the task to the active task manager
            tm.WriteInt64(taskid)
            tm.WriteInt64(runid)
            if task == None:
                tm.WriteInt64(0)
            else:
                tm.WriteInt64(len(task))
                tm.Write(task)

            # Wait for a response
            response = tm.ReadInt64(jm_recv_timeout)

            if response == messaging.msg_send_full:
                # Task was sent, but the task manager is now full
                sent.append((taskid, task))
                task = None
                break

            elif response == messaging.msg_send_more:
                # Continue pushing tasks
                sent.append((taskid, task))
                task = None
                pass

            elif response == messaging.msg_send_rjct:
                # Task was rejected by the task manager, this is not
                # predicted for a model where just one task manager
                # pushes tasks, exit the task loop
                logging.warning('Task manager at %s:%d rejected task %d',
                    tm.address, tm.port, taskid)
                break

            else:
                # The task manager is not replying as expected
                logging.error('Unknown response from the task manager!')
                break
        except:
            # Something went wrong with the connection,
            # try with another task manager
            logging.error('Error pushing tasks to task manager!')
            log_lines(traceback.format_exc(), logging.debug)
            break

    return (False, taskid, task, sent)

###############################################################################
# Read and commit tasks while the task manager is not empty
###############################################################################
def commit_tasks(job, runid, co, tm, tasklist, completed):
    # Keep pulling until finished or the task manager is full
    n_errors = 0
    while True:
        try:
            # Pull the task from the active task manager
            taskid = tm.ReadInt64(jm_recv_timeout)

            if taskid == messaging.msg_read_empty:
                # No more task to receive
                return

            # Read the run id
            taskrunid = tm.ReadInt64(jm_recv_timeout)

            # Read the rest of the task
            r = tm.ReadInt64(jm_recv_timeout)
            ressz = tm.ReadInt64(jm_recv_timeout)
            res = tm.Read(ressz, jm_recv_timeout)

            # Tell the task manager that the task was received
            tm.WriteInt64(messaging.msg_read_result)

            # Warning, exceptions after this line may cause task loss
            # if not handled properly!!

            if r != 0:
                n_errors += 1
                if r == messaging.res_module_error:
                    logging.error('The remote worker crashed while ' +
                        'executing task %d!', r)
                else:
                    logging.error('The task %d was not successfully executed, ' +
                        'worker returned %d!', taskid, r)

            if taskrunid < runid:
                logging.debug('The task %d is from the previous run %d ' +
                    'and will be ignored!', taskid, taskrunid)
                continue

            if taskrunid > runid:
                logging.error('Received task %d from a future run %d!',
                    taskid, taskrunid)
                continue

            # Validated completed task

            c = completed.get(taskid, (None, None))
            if c[0] != None:
                # This may happen with the fault tolerance system. This may
                # lead to tasks being put in the tasklist by the job manager
                # while being committed. The tasklist must be constantly
                # sanitized.
                logging.warning('The task %d was received more than once ' +
                    'and will not be committed again!',
                    taskid)
                # Removed the completed task from the tasklist
                tasklist.pop(taskid, (None, None))
                continue

            # Remove it from the tasklist

            p = tasklist.pop(taskid, (None, None))
            if p[0] == None and c[0] == None:
                # The task was not already completed and was not scheduled
                # to be executed, this is serious problem!
                logging.error('The task %d was not in the working list!',
                    taskid)

            r2 = job.spits_committer_commit_pit(co, res)

            if r2 != 0:
                logging.error('The task %d was not successfully committed, ' +
                    'committer returned %d', taskid, r2)

            # Add completed task to list
            completed[taskid] = (r, r2)

        except:
            # Something went wrong with the connection,
            # try with another task manager
            break
    if n_errors > 0:
        logging.warn('There were %d failed tasks' % (n_errors, ))


def infinite_tmlist_generator():
    ''' Iterates over TMs returned by the load_tm_list() method indefinitely.
    The result of a single iteration is a tuple containing (Finished, Name,
    TM), where Finished == True indicates if the currently listed  TMs
    finished. The next iteration will read the TMs again, setting Finished to
    False.

    Conditions:
        Finished == True <=> (Name, TM) == (None, None)
        Finished == False <=> (Name, TM) != (None, None)

    Example:
        for isEnd, name, tm in infinite_tmlist_generator():
            if not isEnd:
                do something with the task manager (name, tm)
            else:
                all tms where processed, you can do post processing here. The
                next iteration will set isEnd to True and start over again'''
    tmlist = load_tm_list()
    while True:
        try:
            newtmlist = load_tm_list()
            if len(newtmlist) > 0:
                tmlist = newtmlist
            elif len(tmlist) > 0:
                logging.warning('New list of task managers is ' +
                    'empty and will not be updated!')
        except:
            if len(tmlist) > 0:
                logging.warning('New list of task managers is ' +
                    'empty and will not be updated!')
        for name, tm in tmlist.items():
            yield False, name, tm
        yield True, None, None


###############################################################################
# Heartbeat routine
###############################################################################
def heartbeat(finished):
    global jm_heartbeat_interval
    t_last = time.clock()
    for isEnd, name, tm in infinite_tmlist_generator():
        if finished[0]:
            logging.debug('Stopping heartbeat thread...')
            return
        if isEnd:
            t_curr = time.clock()
            elapsed = t_curr - t_last
            t_last = t_curr
            sleep_for = max(jm_heartbeat_interval - elapsed, 0)
            time.sleep(sleep_for)
        else:
            try:
                tm.Open(jm_heart_timeout)
            except:
                # Problem connecting to the task manager
                # Because this is a connection event,
                # make it a debug rather than a warning
                logging.debug('Error connecting to task manager at %s:%d!',
                    tm.address, tm.port)
                log_lines(traceback.format_exc(), logging.debug)
                tm.Close()
                continue
            try:
                # Send the job identifier
                tm.WriteString(jm_jobid)

                # Verify job id of the answer
                jobid = tm.ReadString(jm_recv_timeout)

                if jm_jobid != jobid:
                    logging.error('Job Id mismatch from %s:%d! Self: %s, task manager: %s!',
                        tm.address, tm.port, jm_jobid, jobid)
                    tm.Close()
                    continue

                # Send the heartbeat
                tm.WriteInt64(messaging.msg_send_heart)
            except:
                logging.warning('Error connecting to task manager at %s:%d!',
                    tm.address, tm.port)
                log_lines(traceback.format_exc(), logging.debug)
            finally:
                tm.Close()


###############################################################################
# Job Manager routine
###############################################################################
def jobmanager(argv, job, runid, jm, tasklist, completed):
    logging.info('Job manager running...')
    memstat.stats()

    # Load the list of nodes to connect to
    tmlist = load_tm_list()

    # Store some metadata
    submissions = [] # (taskid, submission time, [sent to])

    # Task generation loop

    taskid = 0
    task = None
    finished = False

    while True:
        # Reload the list of task managers at each
        # run so new tms can be added on the fly
        try:
            newtmlist = load_tm_list()
            if len(newtmlist) > 0:
                tmlist = newtmlist
            elif len(tmlist) > 0:
                logging.warning('New list of task managers is ' +
                    'empty and will not be updated!')
        except:
            logging.error('Failed parsing task manager list!')

        for name, tm in tmlist.items():
            logging.debug('Connecting to %s:%d...', tm.address, tm.port)

            # Open the connection to the task manager and query if it is
            # possible to send data
            if not setup_endpoint_for_pushing(tm):
                finished = False
            else:
                logging.debug('Pushing tasks to %s:%d...', tm.address, tm.port)

                # Task pushing loop
                memstat.stats()
                finished, taskid, task, sent = push_tasks(job, runid, jm,
                    tm, taskid, task, tasklist, completed[0] == 1)

                # Add the sent tasks to the sumission list
                submissions = submissions + sent

                # Close the connection with the task manager
                tm.Close()

                logging.debug('Finished pushing tasks to %s:%d.',
                    tm.address, tm.port)

            if finished and completed[0] == 0:
                # Tell everyone the task generation was completed
                logging.info('All tasks generated.')
                completed[0] = 1

            # Exit the job manager when done
            if len(tasklist) == 0 and completed[0] == 1:
                logging.debug('Job manager exiting...')
                return

            # Keep sending the uncommitted tasks
            # TODO: WARNING this will flood the system
            # with repeated tasks
            if finished and len(tasklist) > 0:
                if len(submissions) == 0:
                    logging.critical('The submission list is empty but '
                        'the task list is not! Some tasks were lost!')

                # Select the oldest task that is not already completed
                while True:
                    taskid, task = submissions.pop(0)
                    if taskid in tasklist:
                        break

        # Remove the committed tasks from the submission list
        submissions = [x for x in submissions if x[0] in tasklist]

        time.sleep(jm_send_backoff)

###############################################################################
# Committer routine
###############################################################################
def committer(argv, job, runid, co, tasklist, completed):
    logging.info('Committer running...')
    memstat.stats()

    # Load the list of nodes to connect to
    tmlist = load_tm_list()

    # Result pulling loop
    while True:
        # Reload the list of task managers at each
        # run so new tms can be added on the fly
        try:
            newtmlist = load_tm_list()
            if len(newtmlist) > 0:
                tmlist = newtmlist
            elif len(tmlist) > 0:
                logging.warning('New list of task managers is ' +
                    'empty and will not be updated!')
        except:
            logging.error('Failed parsing task manager list!')

        for name, tm in tmlist.items():
            logging.debug('Connecting to %s:%d...', tm.address, tm.port)

            # Open the connection to the task manager and query if it is
            # possible to send data
            if not setup_endpoint_for_pulling(tm):
                continue

            logging.debug('Pulling tasks from %s:%d...', tm.address, tm.port)

            # Task pulling loop
            commit_tasks(job, runid, co, tm, tasklist, completed)
            memstat.stats()

            # Close the connection with the task manager
            tm.Close()

            logging.debug('Finished pulling tasks from %s:%d.',
                tm.address, tm.port)

            if len(tasklist) == 0 and completed[0] == 1:
                logging.info('All tasks committed.')
                logging.debug('Committer exiting...')
                return

        # Refresh the tasklist
        for taskid in completed:
            tasklist.pop(taskid, 0)

        time.sleep(jm_recv_backoff)

###############################################################################
# Kill all task managers
###############################################################################
def killtms():
    logging.info('Killing task managers...')

    # Load the list of nodes to connect to
    tmlist = load_tm_list()

    for name, tm in tmlist.items():
        try:
            logging.debug('Connecting to %s:%d...', tm.address, tm.port)

            tm.Open(jm_conn_timeout)

            # Send the job identifier
            tm.WriteString(jm_jobid)

            # Read back the job id of the answer
            tm.ReadString(jm_recv_timeout)

            tm.WriteInt64(messaging.msg_terminate)
            tm.Close()
        except:
            # Problem connecting to the task manager
            logging.warning('Error connecting to task manager at %s:%d!',
                tm.address, tm.port)
            log_lines(traceback.format_exc(), logging.debug)

###############################################################################
# Run routine
###############################################################################
def run(argv, jobinfo, job, runid):
    # List of pending tasks
    memstat.stats()
    tasklist = {}

    # Keep an extra list of completed tasks
    completed = {0: 0}

    # Start the job manager
    logging.info('Starting job manager jor job %d...', runid)

    # Create the job manager from the job module
    jm = job.spits_job_manager_new(argv, jobinfo)

    jmthread = threading.Thread(target=jobmanager,
        args=(argv, job, runid, jm, tasklist, completed))
    jmthread.start()

    # Start the committer
    logging.info('Starting committer for job %d...', runid)

    # Create the job manager from the job module
    co = job.spits_committer_new(argv, jobinfo)

    cothread = threading.Thread(target=committer,
        args=(argv, job, runid, co, tasklist, completed))
    cothread.start()

    # Wait for both threads
    jmthread.join()
    cothread.join()

    # Commit the job
    logging.info('Committing Job...')
    r, res, ctx = job.spits_committer_commit_job(co, 0x12345678)
    logging.debug('Job committed.')

    # Finalize the job manager
    logging.debug('Finalizing Job Manager...')
    job.spits_job_manager_finalize(jm)

    # Finalize the committer
    logging.debug('Finalizing Committer...')
    job.spits_committer_finalize(co)
    memstat.stats()

    if res == None:
        logging.error('Job did not push any result!')
        return messaging.res_module_noans, None

    if ctx != 0x12345678:
        logging.error('Context verification failed for job!')
        return messaging.res_module_ctxer, None

    logging.debug('Job %d finished successfully.', runid)
    return r, res[0]

###############################################################################
# Main routine
###############################################################################
def main(argv):
    # Print usage
    if len(argv) <= 1:
        abort('USAGE: jm module [module args]')

    # Parse the arguments
    args = Args.Args(argv[1:])
    parse_global_config(args.args)

    # Setup logging
    setup_log()
    logging.debug('Hello!')

    # Enable memory debugging
    if jm_memstat == 1:
        memstat.enable()
    memstat.stats()

    # Enable perf module
    if jm_profiling:
        PerfModule(make_uid(), 0, jm_perf_rinterv, jm_perf_subsamp)

    # Load the module
    module = args.margs[0]
    job = JobBinary(module)

    # Remove JM arguments when passing to the module
    margv = args.margs

    # Keep a run identifier
    runid = [0]

    # Wrapper to include job module
    def run_wrapper(argv, jobinfo):
        runid[0] = runid[0] + 1
        return run(argv, jobinfo, job, runid[0])

    # Wrapper for the heartbeat
    finished = [False]
    def heartbeat_wrapper():
        heartbeat(finished)

    # Start the heartbeat
    threading.Thread(target=heartbeat_wrapper).start()

    # Run the module
    logging.info('Running module')
    memstat.stats()
    r = job.spits_main(margv, run_wrapper)
    memstat.stats()

    # Stop the heartbeat thread
    finished[0] = True

    # Kill the workers
    if jm_killtms:
        killtms()

    # Print final memory report
    memstat.stats()

    # Finalize
    logging.debug('Bye!')
    #exit(r)

###############################################################################
# Entry point
###############################################################################
if __name__ == '__main__':
    main(sys.argv)
