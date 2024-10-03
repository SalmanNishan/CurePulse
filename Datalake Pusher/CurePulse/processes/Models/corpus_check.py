import re
import json
import ahocorasick
from fuzzywuzzy import fuzz
from transformers import pipeline
import copy
from processes.Models.utils import Utils
import pandas as pd
import time

class CourpusChecker:
    def __init__(self, cs_corpus_filepath, teng_corpus_filepath):
        self.corpus_flag = False
        self.trie = ahocorasick.Automaton()
        self.sentiment_analysis = pipeline("sentiment-analysis",model="siebert/sentiment-roberta-large-english")
        self.engg_start_index = 0

        with open(cs_corpus_filepath, "r") as f:
            self.negative_corpus = f.readlines()

        with open(teng_corpus_filepath, "r") as f:
            self.teng_teams_corpus = json.load(f)
    
    def check_cs_corpus_sentiment(self, paragraph, filename, date, timestamp, agent_name, client_name, email = False):
        matches = []
        for line in self.negative_corpus:
            match, line = self._find_word_line(line, paragraph)
            if match != '':
                matches.append(match)
            else:
                pass
        matches = set(matches)
        matches = sorted(matches, key=len, reverse=True)

        for match in matches:
            # Use re.sub() to replace each match with an HTML span element
            before_index, after_index = self._find_line(paragraph, match, count_check = False)
            new_paragraph = paragraph[before_index:after_index]
            if not '<span style="color:red">' in new_paragraph:
                paragraph = paragraph.replace(new_paragraph, self._replace_with_red(new_paragraph))
        
        if len(matches) > 0:
            if email == True:
                self._send_email(filename, date, timestamp, agent_name, client_name, paragraph)
            return paragraph
        else:
            return ""
        
    def _find_word_line(self, line, paragraph):
        line_words = line.split()
        num_words = len(line_words)
        words = paragraph.split()

        match_list = []

        for i in range(len(words) - num_words + 1):
            next_words = ' '.join(words[i:i + num_words])
            score = fuzz.ratio(line.lower(), next_words.lower())

            if len(next_words.split()) in [1, 2] and score > 95:
                sentiment = self.sentiment_analysis(next_words)[0]["label"]
                if sentiment == "NEGATIVE":
                    match_list.append({'Score': score, 'Match': next_words, 'Line': line})
            elif score > 85:
                sentiment = self.sentiment_analysis(next_words)[0]["label"]
                if sentiment == "NEGATIVE":
                    match_list.append({'Score': score, 'Match': next_words, 'Line': line})

        if match_list:
            max_score_dict = max(match_list, key=lambda x: x["Score"])
            return max_score_dict["Match"], max_score_dict["Line"]
        else:
            return "", ""
    
    def check_engineering_corpus(self, transcription):
        FOUND = False
        teams = []
        matched = []
        teams_matched_corpus = []

        # Iterate through each team and their corresponding patterns
        for team, patterns in self.teng_teams_corpus.items():
            FOUND_TEAM = False
            team_transcription = copy.deepcopy(transcription)

            # Search for patterns within the transcription
            for pattern in patterns:
                pattern = pattern.strip().lower().replace("\n", "")
                match = re.search(pattern, team_transcription, flags=re.IGNORECASE)
                if match:
                    FOUND_TEAM = True
                    FOUND = True

                    # Highlight matched text in the original transcription
                    before_index, after_index = self._find_line(transcription, pattern)
                    new_transcription = transcription[before_index:after_index]
                    if new_transcription.strip() and '<span style="color:red">' not in new_transcription:
                        transcription = transcription.replace(new_transcription, self._replace_with_red(new_transcription))

                    # Highlight matched text in the team's transcription
                    before_index, after_index = self._find_line(team_transcription, pattern)
                    new_paragraph = team_transcription[before_index:after_index]
                    if new_paragraph.strip() and '<span style="color:red">' not in new_paragraph:
                        team_transcription = team_transcription.replace(new_paragraph, self._replace_with_red(new_paragraph))

                    teams.append(team)
                    matched.append(pattern)

            # Store matched patterns and team information
            teams_matched_corpus.append({
                "Pattern": team_transcription if FOUND_TEAM else '',
                "Team": team
            })

        # Check if any matches were found
        if FOUND:
            if '<b></b></span>' in transcription or '<span style="color:red">' in transcription:
                return "", teams, []
            return transcription, list(set(teams)), teams_matched_corpus
        else:
            return "", teams, []
        
    def _replace_with_red(self, match):
        return f' <span style="color:red"><b>{match}</b></span>'

    def _find_line(self, transcription, word):
        word_index = transcription.find(word, self.engg_start_index)

        newline_before_index = 0
        newline_after_index = 0

        if word_index != -1:
            # Extract the substring before and after the word
            before_word = transcription[:word_index]
            after_word = transcription[word_index+len(word):]

            # Find the last newline character before the word
            newline_before_index = before_word.rfind(".")
            if ": " in before_word[newline_before_index:]:
                newline_before_index = before_word.rfind(": ")
            if newline_before_index == -1:
                newline_before_index = 0

            # Find the first newline character after the word
            newline_after_index = after_word.find(".")
            if "<br>" in after_word[:newline_after_index]:
                newline_after_index = after_word.find("<br>")
            if newline_after_index == -1:
                newline_after_index = len(after_word)

            # Calculate the indices relative to the original string
            newline_before_index += 1
            newline_after_index += word_index + len(word)

        return newline_before_index, newline_after_index
    
    def _send_email(self, filename, date, timestamp, agent_name, client_name, paragraph):
        subject = f'Red Flagged Call {(date)}'
        email_from = f'CurePulse CS Call Alert <aialerts@curemd.com>'
        # email_to = """
		# 	syed.obaid@curemd.com, salman.nishan@curemd.com, clive.archer@curemd.com, kevin.anderson@curemd.com, 
		# 	kamran.ashraf@curemd.com, ishaq.ahmed@curemd.com, abdullah.sohail@curemd.com"""
        importance = 'high'
        email_to = """
			syed.obaid@curemd.com, salman.nishan@curemd.com
        """
        
        action = f'<a href="http://curepulse.curemd.com:5002/test{filename}/{date}?menu=I4d4o0v4h7" target="_blank">Link</a>'
        data = [[filename, timestamp, agent_name, client_name, action]]
        df = pd.DataFrame(data, columns=["Filename", "Call Timestamp", "Agent Name", "Client Name", ""])
        df1 = pd.DataFrame([[paragraph]], columns=["Transcription"])
        style_table_1 = f"""
                    table {{ 
                        border-collapse: collapse; 
                        width: 80%; 
                        margin: auto; 
                        }}
                    th  {{ 
                        text-align: left; 
                        padding: 8px; 
                        font-size: 14px; 
                        }}
                    td {{ 
                        text-align: left; 
                        padding: 8px; 
                        font-size: 12px; 
                        }}
                    th {{ 
                        background-color: #6699CC; 
                        color: white; 
                        }}
                    tr:nth-child(even) {{ 
                        background-color: #f2f2f2; 
                        }} 
            """
        table_html_1 = '<style>{}</style>'.format(style_table_1, df.to_html(index=False, escape=False))
        message1 = f"""
                Hi,
                <br><br>
                Please find attached the call details flagged as red for the date {date}.
                <br><br>
                {df.to_html(index=False, escape=False)}
                <br>
                {df1.to_html(index=False, escape=False)}
                <br><br>
                Regards,
                <br>
                CurePulse Admins
                <br><br>

                <u><b>Note: This is an automated email. Do not reply on this.</b></u>
        """
        html = '<html><body><p>' + table_html_1 + message1 + '</p></body></html>'
        Utils.send_email(subject, email_from, email_to, html, importance)
