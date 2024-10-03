from flask import Flask, request, redirect, render_template, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import json
import pandas as pd

app = Flask(__name__)
app.secret_key = 'your_very_secret_key_here'

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

users = {
    'admin': {'password': 'adminpass', 'role': 'admin'},
    'user': {'password': 'userpass', 'role': 'user'}
}

FILE_PATH_TEAMS = 'CS_Teams_Data.json'
FILE_PATH_CLINET_FACING = 'client_facing_agents_hierarchy.json'
FILE_PATH_NON_CLINET_FACING = 'non_client_facing_agents_hierarchy.json'

class User(UserMixin):
    def __init__(self, id, role):
        self.id = id
        self.role = role

    def is_admin(self):
        return self.role == 'admin'

@login_manager.user_loader
def load_user(user_id):
    user = users.get(user_id)
    if user:
        return User(id=user_id, role=user.get('role'))
    return None

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = users.get(username)
        if user and user['password'] == password:
            user_obj = User(id=username, role=user['role'])
            login_user(user_obj)
            return redirect(url_for('index'))
        flash('Invalid credentials')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    team_data = read_json(FILE_PATH_TEAMS)
    all_managers = extract_managers(FILE_PATH_NON_CLINET_FACING, FILE_PATH_CLINET_FACING)
    teams = list(team_data.keys())  # Extract the keys which are team names
    return render_template('index.html', teams=teams, managers=all_managers)

@app.route('/add_user', methods=['POST'])
@login_required
def add_user():    
    name = request.form['user_name']
    user_name = convert_name_to_username(name)
    team_name = request.form['team_name']
    ext = request.form['ext']
    managers = request.form.getlist('managers')

    update_extension(name, ext)
    update_team(user_name, team_name)
    update_managers(user_name, managers)
    return redirect(url_for('index'))

def update_extension(name, ext):
    df = pd.read_excel("/home/cmdadmin/Datalake Pusher/config/CS_Names_Extensions.xlsx")
    new_row = {'Name': name, "Ext": ext}
    df = df.append(new_row, ignore_index=True)
    df.to_excel("/home/cmdadmin/Datalake Pusher/config/CS_Names_Extensions.xlsx", index=False)

def update_managers(user_name, managers):
    data = read_json(FILE_PATH_CLINET_FACING)
    filename = FILE_PATH_CLINET_FACING

    if not is_manager_in_data(managers[0], data):
        data = read_json(FILE_PATH_NON_CLINET_FACING)
        filename = FILE_PATH_NON_CLINET_FACING

    # Add the new instance at the end with "name" key
    name_instance = {user_name: managers}
    if isinstance(data, list):
        # If the JSON data is a list, we append the new instance to it
        data.append(name_instance)
    elif isinstance(data, dict):
        # If the JSON data is a dictionary, we need to decide how to append.
        # This will add a new key at the outermost level of the JSON structure.
        data[user_name] = managers
    else:
        raise TypeError("JSON data is neither a list nor a dictionary.")
    
    # Write the modified data back to the file
    write_json(filename, data)

def update_team(user_name, team_name):
    data = read_json(FILE_PATH_TEAMS)
    if team_name in data:
        # Assuming each team is a list of users, append the new user name
        data[team_name].append(user_name)
    else:
        # Create a new team with the user
        data[team_name] = [user_name]
    
    # Write the modified data back to the file
    write_json(FILE_PATH_TEAMS, data)

def is_manager_in_data(manager, data):
    # Iterate over all values in the dictionary
    for value in data.values():
        # Check if the value is a list and the manager is in this list
        if isinstance(value, list) and manager in value:
            return True
    return False

def convert_name_to_username(name):
    # Remove any leading/trailing whitespace
    name = name.strip()
    # Replace spaces with dots or underscores
    username = name.replace(" ", ".").lower()
    return username

def extract_managers(*filenames):
    managers_set = set()
    for filename in filenames:
        data = read_json(filename)
        for managers in data.values():
            managers_set.update(managers)
    return managers_set

def read_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def write_json(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

if __name__ == '__main__':
    app.run(debug=False, host='172.16.101.152', port=5005)
