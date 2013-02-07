add_definitions(-DMPI_BUILD)
add_library(agent OBJECT agent_helper.c agent_sm.c fdf_agent.c)
add_dependencies(agent ffdc)
