import csv
import random

# Lists of sample data
first_names = [
    "John", "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia",
    "James", "Isabella", "Benjamin", "Mia", "Lucas", "Charlotte", "Mason",
    "Amelia", "Ethan", "Harper", "Logan", "Evelyn", "Oliver", "Abigail",
    "Elijah", "Emily", "Aiden", "Elizabeth", "Daniel", "Sofia", "Matthew",
    "Ella", "Henry", "Grace", "Jackson", "Chloe", "Samuel", "Victoria",
    "David", "Aria", "Joseph", "Avery", "Owen", "Lily", "Wyatt", "Hannah",
    "Sebastian", "Eleanor", "Caleb", "Zoey", "Ryan", "Lillian", "Andrew",
    "Nora", "Joshua", "Scarlett", "Christian", "Grace", "Jonathan", "Victoria",
    "Dylan", "Penelope", "Thomas", "Riley", "Landon", "Isaac", "Gabriel",
    "Stella", "Anthony", "Aurora", "Christian", "Natalie", "Eli", "Layla",
    "Isaiah", "Hazel", "Charles", "Violet", "Connor", "Addison", "Aaron",
    "Eleanor", "Adrian", "Lucy", "Thomas", "Audrey", "Colton", "Brooklyn",
    "Jeremiah", "Claire", "Easton", "Anna", "Cameron", "Hailey", "Robert",
    "Samantha", "Nicholas", "Zoe", "Brayden", "Madison"
]

last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis",
    "Garcia", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson",
    "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee",
    "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez",
    "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright",
    "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
    "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter",
    "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Edwards", "Collins", "Stewart", "Morris", "Rogers", "Reed", "Cook",
    "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper", "Richardson",
    "Cox", "Howard", "Ward", "Brooks", "Chavez", "Wood", "James", "Bennett",
    "Gray", "Mendoza", "Ruiz", "Hughes", "Price", "Peterson", "Powell",
    "Jenkins", "Long", "Foster", "Perry", "Ross", "Barnes", "Fisher",
    "Henderson", "Coleman", "Jenkins", "Simmons", "Patterson", "Jordan",
    "Reynolds", "Hamilton", "Graham","Dauphine"
]

countries = [
    "United States", "Canada", "Australia", "United Kingdom", "New Zealand",
    "Ireland", "South Africa", "Spain", "Mexico", "Argentina", "Chile",
    "Colombia", "South Korea", "Germany", "France", "Italy", "Netherlands",
    "Sweden", "Norway", "Denmark", "Brazil", "Japan", "China", "India",
    "Russia", "Portugal", "Belgium", "Switzerland", "Austria", "Finland",
    "Poland", "Czech Republic", "Hungary", "Greece", "Turkey", "Israel",
    "Saudi Arabia", "United Arab Emirates", "Egypt", "Nigeria", "Kenya",
    "Indonesia", "Malaysia", "Philippines", "Vietnam", "Thailand",
    "Singapore", "Argentina", "Peru", "Venezuela"
]

# Function to generate a random email
def generate_email(first_name, last_name):
    domains = ["example.com", "sample.org", "demo.net", "test.com"]
    return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"

# Generate and write data to CSV
with open('dummy_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
    # Write header
    writer.writerow(["ID", "FirstName", "LastName", "Age", "Email", "Country"])
    for i in range(1, 1000001):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        age = random.randint(18, 65)
        email = generate_email(first_name, last_name)
        country = random.choice(countries)
        writer.writerow([i, first_name, last_name, age, email, country])