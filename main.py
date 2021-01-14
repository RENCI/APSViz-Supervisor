from supervisor.src.supervisor import APSVizSupervisor

# create the supervisor
supervisor = APSVizSupervisor()

# initiate the polling for work
supervisor.run()
