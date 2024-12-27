import matplotlib.pyplot as plt
from botbuilder.core import MessageFactory
from botbuilder.schema import Attachment
from datetime import datetime


def create_adaptive_card(chart_base64):

    adaptive_card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "Image",
                "url": f"data:image/png;base64,{chart_base64}",
                "size": "stretch",
                "altText": "Generated Sales Chart",
            },
        ],
    }

    # Create an attachment from the adaptive card JSON
    card_attachment = Attachment(
        content_type="application/vnd.microsoft.card.adaptive",
        content=adaptive_card,
    )
    return card_attachment
