import re
import unicodedata
import os
import anthropic

class HebrewProcessor:
    def __init__(self):
        self.year_patterns = [
            r'ממוצע \d{4}', r'סוף \d{4}', r'\d{4}'
        ]
        self.api_key = os.getenv('CLAUDE_API_KEY')
        self.client = anthropic.Anthropic(api_key=self.api_key) if self.api_key else None

        # Only normalize truly equivalent terms, not substantive differences
        self.safe_normalizations = [
            (r'ושיעור', 'ואחוז'),  # These are truly synonymous
            (r'\s+', ' '),  # Multiple spaces to single space
            (r'־', '-'),  # Different dash types
        ]

    def process_header(self, text):
        text = unicodedata.normalize('NFC', text)
        text = re.sub(r'[\u0591-\u05C7]', '', text)

        # Use API if available and text looks repetitive
        if self.client and self._looks_repetitive(text):
            text = self._clean_with_api(text)

        # Apply only safe normalizations
        for pattern, replacement in self.safe_normalizations:
            text = re.sub(pattern, replacement, text)

        # Remove years, continuation markers, and table numbers
        for pattern in self.year_patterns:
            text = re.sub(pattern, '', text)
        text = re.sub(r'\(המשך\)', '', text)
        text = re.sub(r'לוח:\s*\d+\.\d+', '', text)

        return ' '.join(text.split()).strip()

    def _looks_repetitive(self, text):
        """Check if text appears to have repetition"""
        return len(text) > 200 and text[:50] in text[50:]

    def _clean_with_api(self, text):
        """Use Claude to intelligently deduplicate"""
        try:
            response = self.client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=200,
                messages=[{
                    "role": "user",
                    "content": f"Remove duplicate repetitions from this Hebrew text, keeping only one occurrence. Preserve all meaningful words like מספר, אחוז, שיעור: {text[:500]}"
                }]
            )
            return response.content[0].text
        except:
            return text  # Fallback to original if API fails