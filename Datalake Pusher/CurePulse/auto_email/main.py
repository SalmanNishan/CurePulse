import pandas as pd
from data import CurePulseData
from write_csv import CSVWriter
from send_email import send_email
from datetime import datetime


def forward(dept, cp_data, writer, to):
	outgoing_voip = cp_data.get_total_voip_calls_count(cp_data.outgoing_dataframe)
	incoming_voip = cp_data.get_total_voip_calls_count(cp_data.incoming_dataframe)
	outgoing_processed = cp_data.get_processed_calls_count("outgoing")
	incoming_processed = cp_data.get_processed_calls_count("incoming")
	writer.write_calls_count(outgoing_voip, incoming_voip, outgoing_processed, incoming_processed)

	processed_calls = cp_data.get_total_calls()
	calls_made = cp_data.get_calls_made()
	incoming  =cp_data.get_incoming_calls()
	outgoing = cp_data.get_outgoing_calls()
	time = cp_data.total_calls_time()
	agent_score = cp_data.get_agent_score()
	client_score = cp_data.get_client_score()
	agent_positive= cp_data.agent_positive_count()
	agent_negative = cp_data.agent_negative_count()
	agent_neutral = cp_data.agent_neutral_count()
	client_positive = cp_data.client_positive_count()
	client_negative = cp_data.client_negative_count()
	client_neutral = cp_data.client_neutral_count()
	writer.write_first_rows(processed_calls, calls_made, outgoing, incoming, time, agent_score, client_score, agent_positive, agent_negative, 
                                agent_neutral, client_positive, client_negative, client_neutral)
	
	total_agents = cp_data.get_total_agents()
	average_calls_per_agent = cp_data.get_average_calls_per_agent(total_agents, calls_made)
	average_talktime_per_agent = cp_data.average_talktime_per_agent(total_agents)
	writer.write_second_row(total_agents, average_calls_per_agent, average_talktime_per_agent)
    
	top_agents = cp_data.get_top_5_agents()
	bottom_agents = cp_data.get_bottom_5_agents()
	writer.write_5_agents(top_agents, bottom_agents)


	top_clients = cp_data.get_top_5_clients()
	botton_clients = cp_data.get_bottom_5_clients()
	writer.write_5_cleints(top_clients, botton_clients)

	date = cp_data.get_date()
	all_data, _ = cp_data.get_all_data()
	writer.write_all_data(all_data)
			
	file_name = writer.file_name
	writer.close_file()

	file_name = writer.csv_to_excel()

	performance_score_table = cp_data.get_agents_average_scores(file_name)

	scores = pd.DataFrame([(client_score, agent_score)], columns=['Client Satisfaction Score', 'Agent Performance Score'])

	below_60_clients = pd.DataFrame(cp_data.below_60_clients(), columns=['Client Name', 'Score'])
	below_60_agents = cp_data.below_60_agents(performance_score_table)

	date = cp_data.get_date()
	calls_count = pd.DataFrame([{"Calls Made (Outgoing + Incoming)": calls_made, "Calls Processed (Outgoing + Incoming)": processed_calls,
			      "Percentage Processed": f"{str(round((0 if calls_made == 0 else min(processed_calls/calls_made, 1))*100))}%"}])
	send_email(date, file_name, scores.to_html(index=False), below_60_clients.to_html(index=False), below_60_agents.to_html(index=False), dept, to,
	    	calls_count.to_html(index=False))
	
if __name__ == '__main__':
	today = datetime.today()
	if 1 <= today.weekday() <= 5:
		url = "mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1"
		db = 'CurePulse'
		collection = 'CurePulse_Processed_Calls'
		CURE_PULSE_DATA = CurePulseData(url, db, collection, "CS")
		date = CURE_PULSE_DATA.get_date()
		CSV_WRITER = CSVWriter(date)
		# to = "syed.obaid@curemd.com, salman.nishan@curemd.com"
		to = """
				syed.obaid@curemd.com, salman.nishan@curemd.com, hamza.ansar@curemd.com, 
				muhammad.ahsan@curemd.com, kashif.latif@curemd.com, muddassar.farooq@curemd.com, 
				saqlain.nawaz@curemd.com, adnan.malik@curemd.com, ray.parker@curemd.com, 
				clive.archer@curemd.com, kevin.anderson@curemd.com, kamran.ashraf@curemd.com, 
				ishaq.ahmed@curemd.com, shujaat.khan@curemd.com, imran.hussain@curemd.com, 
				james.parks@curemd.com, abdullah.sohail@curemd.com
				"""
		forward("CS", CURE_PULSE_DATA, CSV_WRITER, to)

		CURE_PULSE_DATA = CurePulseData(url, db, collection, "Sales")
		date = CURE_PULSE_DATA.get_date()
		CSV_WRITER = CSVWriter(date)
		to = """
				syed.obaid@curemd.com, salman.nishan@curemd.com, hamza.ansar@curemd.com, 
				muhammad.ahsan@curemd.com, kashif.latif@curemd.com, muddassar.farooq@curemd.com, 
				saqlain.nawaz@curemd.com, adnan.malik@curemd.com, mike.jones@curemd.com, 
				eddie@curemd.com, abdullah.sohail@curemd.com
				"""
		forward("Sales", CURE_PULSE_DATA, CSV_WRITER, to)