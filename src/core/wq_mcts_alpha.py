import random
import time
from datetime import datetime
import pandas as pd
from src.core.wq_alpha_analysis import AlphaTracker
from openai import OpenAI  # or use your LLM client wrapper

class MCTSNode:
    def __init__(self, expression, parent=None):
        self.expression = expression
        self.parent = parent
        self.children = []
        self.visits = 0
        self.value = 0.0  # Average IC or fitness

class MCTSAlphaExplorer:
    def __init__(self, tracker: AlphaTracker, llm_client, exploration_param=1.0):
        self.tracker = tracker
        self.llm_client = llm_client
        self.exploration_param = exploration_param
        self.root = None

    def uct(self, node):
        if node.visits == 0:
            return float('inf')
        parent_visits = node.parent.visits if node.parent else 1
        return node.value + self.exploration_param * ( (2 * (parent_visits ** 0.5)) / node.visits )

    def select(self, node):
        if not node.children:
            return node
        return max(node.children, key=self.uct)

    def expand(self, node):
        prompt = f"""You are a quant researcher. Modify the following alpha expression by changing one subcomponent, keeping the same economic intuition:\n{node.expression}"""
        try:
            response = self.llm_client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
            )
            new_exprs = response.choices[0].message.content.strip().split("\n")
            for expr in new_exprs:
                expr = expr.strip().lstrip("1234567890. ").strip()
                if expr:
                    child = MCTSNode(expr, parent=node)
                    node.children.append(child)
        except Exception as e:
            print("LLM error:", e)

    import time
    import random
    from datetime import datetime
    import pandas as pd

    def simulate(self, node, data_field="adv20"):
        code = node.expression.replace("{data_field}", data_field)
        idea_id = f"mcts_{hash(code)}_{random.randint(1000, 9999)}"
        now = datetime.now()
        new_row = pd.DataFrame([{
            "idea_id": idea_id,
            "description": "LLM MCTS variant",
            "template": node.expression,
            "code": code,
            "data": data_field,
            "creation_date": now,
            "last_updated": now,
            "status": "pending",
        }])
        self.tracker.append_tracker(new_row)
        self.tracker.save_tracker()
        print(f"Submitted alpha {idea_id} for simulation")

        # Wait for simulation result (poll every 30s, up to 10 minutes)
        for _ in range(20):  # max 10 mins
            time.sleep(30)
            self.tracker.load_tracker()
            result_row = self.tracker.df[self.tracker.df['idea_id'] == idea_id]
            if not result_row.empty and pd.notna(result_row.iloc[0]['sharpe']):
                reward = result_row.iloc[0]['sharpe']
                print(f"Received backtest result for {idea_id}: sharpe={reward}")
                return reward

        print(f"Timed out waiting for result of {idea_id}, using fallback reward = 0.0")
        return 0.0

    def backpropagate(self, node, reward):
        while node:
            node.visits += 1
            node.value += (reward - node.value) / node.visits
            node = node.parent

    def run(self, base_expr, num_iterations=10):
        self.root = MCTSNode(base_expr)
        for _ in range(num_iterations):
            node = self.select(self.root)
            self.expand(node)
            for child in node.children:
                reward = self.simulate(child)
                self.backpropagate(child, reward)
                time.sleep(1)  # throttle if needed
