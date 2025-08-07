
"""
{"Tale of Two Cities": "Charles Dickens", "One Hundred Years of Solitude": "Gabriel García Márquez", 
"The Great Gatsby": "F. Scott Fitzgerald", "Ulysses": "James Joyce", "The Catcher in the Rye": "J. D. Salinger", 
"A Portrait of the Artist as a Young Man": "james joyce", "Moby-Dick": "Herman Melville", "A Christmas Carol": "Charles Dickens"}

 

Write a function that creates a new structure that contains the author’s name as the key and the values is the list of books they have written."""



def process_dictonary(input_dictionary):
    
    output = {} 
    
    
    for  book,author in input_dictionary.items():
        if author in output:
            output[author].append(book)
        else:
            output[author]=[book]
  
    
    print(output)
input_dictionary ={"Tale of Two Cities": "Charles Dickens", "One Hundred Years of Solitude": "Gabriel García Márquez", 
"The Great Gatsby": "F. Scott Fitzgerald", "Ulysses": "James Joyce", "The Catcher in the Rye": "J. D. Salinger", 
"A Portrait of the Artist as a Young Man": "james joyce", "Moby-Dick": "Herman Melville", "A Christmas Carol": "Charles Dickens"}

process_dictonary(input_dictionary)

# Expected Output:  {'Charles Dickens': ['Tale of Two Cities', 'A Christmas Carol'], 'Gabriel García Márquez': ['One Hundred Years of Solitude'], 'F. Scott Fitzgerald': ['The Great Gatsby'], 'James Joyce': ['Ulysses', 'A Portrait of the Artist as a Young Man'], 'J. D. Salinger': ['The Catcher in the Rye'], 'Herman Melville': ['Moby-Dick']}