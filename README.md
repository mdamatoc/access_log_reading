# access_log_reading

###**What Is This?**
This is a simple Python script intended to provide the answers of Multiplan's Data Engineer exam.

###**Required Softwares and Applications**
Pyenv 1.2.*
Python 3.8.*  
Pip 19.2.*  


###**Requirements**
Create a virtual environment at the same folder of this project using _pyenv virtualenv 3.8.3 access_log_reading_  
Activate the virtualenv environment using _pyenv local access_log_reading_
The commands above can be substituted by any other virtual environment.    

Install the libraries needed to run the project. They're listed in requirements.txt. Try _python -m pip install -r requirements.txt --require-hashes_  
Note that libraries in requirements are specified with hashes because with that, there is no worries with DNSs problems.

###**How To Run The Application**
python main.py


###**What I Should Expect of the Application?**
There are 5 questions, called in the applications by 'items'.

Each question will generate a single file, .csv or .json, that answers the asked items.
Each file of these will start with 'item_X', where X can be 1, 2, 3, 4 or 5. Each files reproduces the asked actions in the item equivalent.