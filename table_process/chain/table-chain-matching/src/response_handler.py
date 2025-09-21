from enum import Enum

class DecisionAction(Enum):
    CONFIRM = "confirm"
    REJECT = "reject"
    SPLIT = "split"
    MERGE = "merge"
    MANUAL = "manual"

class APIResponseHandler:
    def __init__(self):
        self.decisions = []
        self.manual_queue = []

    def process_response(self, api_response, match_type):
        """Process API validation response"""
        decision = api_response.get('decision', 'uncertain')
        confidence = api_response.get('confidence', 0.5)

        if decision == 'accept' and confidence >= 0.7:
            action = DecisionAction.CONFIRM
        elif decision == 'reject' and confidence >= 0.7:
            action = DecisionAction.REJECT
        else:
            action = DecisionAction.MANUAL
            self.manual_queue.append(api_response)

        self.decisions.append({
            'action': action,
            'confidence': confidence,
            'type': match_type,
            'response': api_response
        })

        return action