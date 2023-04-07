from typing import cast
import click
import importlib
import textwrap

def echo_cli_flag():
    click.echo(textwrap.dedent('''
                                _  __               _____ _      _____ 
     /\                        (_)/ _|             / ____| |    |_   _|
    /  \   ___ _   _ _ __   ___ _| |_ _   _ ______| |    | |      | |  
   / /\ \ / __| | | | '_ \ / __| |  _| | | |______| |    | |      | |  
  / ____ \\__ \ |_| | | | | (__| | | | |_| |      | |____| |____ _| |_ 
 /_/    \_\___/\__, |_| |_|\___|_|_|  \__, |       \_____|______|_____|
                __/ |                  __/ |                           
               |___/                  |___/                                
    '''))


def import_queue(queue: str):
    import sys, os
    sys.path.append(os.getcwd())
    
    q_split = queue.split(".")
    queue_instance_name = q_split[-1]
    queue_module_path = '.'.join(q_split[:-1])
    try:
        queue_module = importlib.import_module(queue_module_path)
        queue_instance = getattr(queue_module, queue_instance_name)
    except ModuleNotFoundError as e:
        click.echo("[error] module {} not found".format(queue_module_path))
        raise e
    except Exception as e:
        click.echo("[error] module import failed, please check")
        raise e
    from asyncify.queue_ import Queue
    if not isinstance(queue_instance, Queue):
        raise Exception('[error] {} is not `asyncify.Queue` type".format(queue_instance)')
    return queue_instance

@click.group()
@click.option("--queue", required=True)
@click.pass_context
def asyncify_cli(ctx, queue: str):
    """asyncify cli"""
    echo_cli_flag()
    queue_instance = import_queue(queue)
    ctx.obj = {}
    ctx.obj["queue_instance"] = queue_instance
    

@asyncify_cli.command()
@click.pass_context
def queue_info(ctx):
    queue_instance = ctx.obj["queue_instance"]
    click.echo(f"[info]:{queue_instance.__name__}\n")
    for task_ident in queue_instance.callable_ident_map.keys():
        click.echo("[+]register task: {}".format(task_ident))

@asyncify_cli.command()
@click.pass_context
def consumer(ctx):
    from asyncify.queue_ import Queue
    from asyncify.consumer import Consumer
    queue_instance = cast(Queue, ctx.obj["queue_instance"])
    consumer = Consumer(queue_instance)
    consumer.run()


if __name__ == "__main__":
    asyncify_cli()