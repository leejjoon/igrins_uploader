import curio
# from multiprocessing import Process, Queue
import time
from curio import Channel
from multiprocessing.connection import Client


async def wait_for_timeout_with_channel(c, timeout_second):
    try:
        async with curio.timeout_after(timeout_second):
            print("start waiting")
            msg = await c.recv()

            if msg == "quit" or msg is None:
                return -1
            else:
                return 1
    except curio.TaskTimeout:
        print("timeout")
        return 0


async def timeout_restart(address, timeout_second, task, *args):

    ch = Channel(address)

    needs_start_up = True
    do_loop = True

    while do_loop:
        if needs_start_up:
            # q = UniversalQueue()
            needs_start_up = False

            upload_task = await curio.spawn(start_task, address,
                                            task, *args)

            c = await ch.accept(authkey=b'peekaboo')
            print("start listening")
            # ch.connect(authkey=b'peekaboo')

        r = await wait_for_timeout_with_channel(c, timeout_second)
        print(r)

        if r == -1:
            do_loop = False

        elif r == 0:
            # restart
            await upload_task.cancel()
            needs_start_up = True


async def start_task(address, task, *args):
    fn_list = []
    try:
        task = await curio.run_in_process(curio_process_task,
                                          address, task, *args)
    except curio.TaskError:
        print ("task error")
        raise

def curio_process_task(address, task, *args):
    print ("connecting", address)
    c = Client(address, authkey=b'peekaboo')
    print ("connected")

    def _cb():
        c.send("tick")

    task(*args, tick_callback=_cb)
    c.send("quit")
    c.close()

def upload_to_drive_test(fn_list, tick_callback=None):

    time.sleep(1)
    if tick_callback is not None:
        tick_callback()
    time.sleep(5)

if __name__ == '__main__':
    address = ("localhost", 3001)
    timeout_second = 3
    fn_list = []
    curio.run(timeout_restart(address, timeout_second,
                              upload_to_drive_test, fn_list))
