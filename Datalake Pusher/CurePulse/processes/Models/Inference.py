import librosa

class Inference():
    def __init__(self, speech_stars, speech_scores, text_stars, audio_filepath, agent = True):
        self.speechStars = speech_stars
        self.speechScores = speech_scores
        self.textStars   = text_stars
        #dividing by 60 for conversion to minutes
        self.Duration = librosa.get_duration(filename=audio_filepath) / 60
        self.agent = agent  # attribute to check if the engine is being run for agent 

    def InferenceEngine(self, config, text_sentiments_count, holding_time = None, grammar_stars = None, accent_stars = None, client_score = None):
        speech_score = self.speechStars * 0.2
        text_score = self.textStars * 0.2

        #contribution of holding time goes towards client sentiment only
        hold_time_stars = 5
        total_hold_time = 0
        thresholds = config.holdtime_thresholds
        if self.agent:
            accent_score = accent_stars * 0.2
            if self.Duration > 3:
                total_hold_time = float(holding_time)/60
                for stars, condition in thresholds.items():
                    if eval(condition):
                        hold_time_stars = stars   
                        break 
            else:
                config.Speech_weight_agent += 0.05
                config.Accent_Weight_agent += 0.05
                config.holding_weight_agent = 0

            total_score_unscaled = (config.Speech_weight_agent * speech_score) + (config.Text_weight_agent * text_score) + \
                                   (config.Grammar_Weight_agent * grammar_stars * 0.2) + (config.Accent_Weight_agent * accent_score) + \
                                   (config.Client_Weight_agent * client_score) + (config.holding_weight_agent * hold_time_stars * 0.2)
            
            #scale the score to be in form of decimal percentage
            total_score = total_score_unscaled / (config.Speech_weight_agent + config.Text_weight_agent + config.Grammar_Weight_agent + config.Accent_Weight_agent + config.Client_Weight_agent + config.holding_weight_agent)

            return total_score, total_hold_time, hold_time_stars
        
        else:
            base_score = config.Speech_weight_client * speech_score + config.Text_weight_client * text_score 
        
            total_score = base_score / (config.Speech_weight_client + config.Text_weight_client)

        return total_score
