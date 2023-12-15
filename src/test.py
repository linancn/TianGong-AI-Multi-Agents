from agents.agent_env import env_agent

env_agent = env_agent()

response = env_agent.invoke(
    {"input": "应该允许国外的废旧电池进口吗？search internet", "chat_history": []}
)

print(response)

# for chunk in env_agent.stream({"input": "应该允许国外的废旧电池进口吗？search internet", "chat_history": []}):
#     if chunk:
#     print(chunk)