package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type Response struct {
    Message     string  `json:"message"`
}

type Book struct {
    Id      int     `json:"id"`
    Title   string  `json:"title"` 
    Author  string  `json:"author"`
    Price   float64 `json:"price"`
}

// For single Book response (create, get by Id, update).
type BookResponse struct {
    Status  string   `json:"status"`
    Message string   `json:"message"`
    Data    Book     `json:"data,omitempty"`
}

// For multiple books operations (GET all, Search).
type BooksResponse struct{
    Status  string   `json:"status"`
    Message string   `json:"message"`
    Data    []Book   `json:"data,omitempty"`
}

// Global DB handler.
var db *sql.DB

// Other endpoints.
func createBookHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    var book Book
    // Checks for invalid req.body.
    err := json.NewDecoder(r.Body).Decode(&book)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Invalid request body.",
        })
        return
    }

    // Checks for empty input field.
    if book.Title == "" || book.Author == "" {
        w.WriteHeader(http.StatusBadRequest)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Title and Author field are required.",
        })
        return
    }

    // Prepare SQL statement.
    stmt, err := db.Prepare("INSERT INTO books (title, author, price) VALUES (?, ?, ?)")
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Database error.",
        })
        log.Printf("Statement preparation error: %v", err)
        return
    }
    defer stmt.Close()

    // Execute Statement.
    result, err := stmt.Exec(book.Title, book.Author, book.Price)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Error creating book",
        })
        log.Printf("Statement execution error: %v", err)
        return
    }

    // Get the lastly inseted bookId
    lastId, err := result.LastInsertId()
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Error getting new book ID.",
        })
        return
    }

    // Set the ID in our Book struct.
    book.Id = int(lastId)

    // Success Response.
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(BookResponse{
        Status: "sucess",
        Message: "Book created Successfully",
        Data: book,
    })
}

func getAllBooksHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    // Query to get-All-Books
    rows, err := db.Query("SELECT id, title, author, price FROM books")
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BooksResponse {
            Status: "error",
            Message: "Error fetching books from database",
        })
        log.Printf("Database query error: %v", err)
        return
    }
    defer rows.Close()

    // Slice to store all Book
    var books []Book

    for rows.Next() {
        var book Book
        err := rows.Scan(&book.Id, &book.Title, &book.Author, &book.Price)
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(BooksResponse{
                Status: "error",
                Message: "Error scanning database records",
            })
            log.Printf("Row scanning error: %v", err)
            return
        }
        books = append(books, book)
    }

    // Check for errors from iterating over rows
    if err = rows.Err(); err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BooksResponse{
            Status: "erro",
            Message: "Error iterating through records",
        })
        log.Printf("Row iteration error: %v", err)
        return
    }

    // If not books found, 
    // return empty array with success status
    if len(books) == 0 {
        w.WriteHeader(http.StatusOK)
        json.NewEncoder(w).Encode(BooksResponse{
            Status: "success",
            Message: "No books found",
            Data: []Book{},
        })
        return
    }

    // Sucess response with books
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(BooksResponse{
        Status: "success",
        Message: "Books retrived sucessfully",
        Data: books,
    })
}

func deleteAllBooks(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    // Execute DELETE query
    result, err := db.Exec("DELETE FROM books")
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(Response{
            Message: "Error deleting books from database", 
        })
        log.Printf("Databse deletion error: %v", err)
        return
    }

    // Get the number of affected rows
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(Response{
            Message: "Error getting affected rows count",
        })
        log.Printf("Error getting affected rows: %v", err)
        return
    }

    // If no books were affected
    if rowsAffected == 0 {
        w.WriteHeader(http.StatusOK)
        json.NewEncoder(w).Encode(Response{
            Message: "No books to delete",
        })
        return
    }
     // Sucess response
     w.WriteHeader(http.StatusOK)
     json.NewEncoder(w).Encode(Response{
         Message: "All books deleted successfully",
     })
}

func updateBookHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    // GET id from URL parameters
    vars := mux.Vars(r)
    id, ok := vars["id"]
    if !ok {
        w.WriteHeader(http.StatusBadRequest)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Book ID is required",
        })
        return
    }

    // Parse request body
    var book Book
    err := json.NewDecoder(r.Body).Decode(&book)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Invalid request body",
        })
        return
    }

    // Check if books exists
    var existingBook Book
    err = db.QueryRow("SELECT id, title, author, price FROM books WHERE id = ?", id).Scan(&existingBook.Id, &existingBook.Title, &existingBook.Author, &existingBook.Price)
    if err == sql.ErrNoRows {
        w.WriteHeader(http.StatusNotFound)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Book no found",
        })
        return
    } else if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Database error while checking book existence",
        })
        log.Printf("Database query error: %v", err)
        return
    }

    // Prepare update quer - only update not-empty fields
    query := "UPDATE books SET"
    var updates []interface{}
    var setParts []string

    if book.Title != "" {
        setParts = append(setParts, " title = ?")
        updates = append(updates, book.Title)
    }
    if book.Author != "" {
        setParts = append(setParts, " author = ?")
        updates = append(updates, book.Author)
    }
    if book.Price != 0 {
        setParts = append(setParts, " price = ?")
        updates = append(updates, book.Price)
    }

    // If no fields to update
    if len(updates) == 0 {
        w.WriteHeader(http.StatusBadRequest)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "No fields to update",
        })
        return
    }

    // Complete the query
    query += strings.Join(setParts, ",") + "WHERE id = ?"
    updates = append(updates, id)

    // Execute update
    result, err := db.Exec(query, updates...)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Error updating book",
        })
        log.Printf("Database update error: %v", err)
        return
    }

    // Check the affected-rows
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Error checking update Status",
        })
        log.Printf("Error getting affected rows: %v", err)
        return
    }

    if rowsAffected == 0 {
        w.WriteHeader(http.StatusNotFound)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Book not found or no changes made",
        })
        return
    }

    // Fetch updated book
    var updatedBook Book
    err = db.QueryRow("SELECT id, title, author, price FROM books WHERE id = ?").Scan(&updatedBook.Title, &updatedBook.Author, &updatedBook.Price)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(BookResponse{
            Status: "error",
            Message: "Error fetching updated book",
        })
        log.Printf("Error fetchingg updated book: %v", err)
        return
    }

    // Success response
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(BookResponse{
        Status: "success",
        Message: "Book updated successfully",
        Data: updatedBook,
    })
}


// Initialize Database connection.
func initDB() {
    var err error

    db, err = sql.Open("mysql", "root:mysecret@tcp(localhost:3306)/bookstore")
    if err != nil {
        log.Fatal(err)
    }

    err = db.Ping()
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Connected to Mysql container.")
}

// Heartbeat program to checkServer.
func checkServer(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    response := Response {
        Message: "Hello, there",
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(response)
}


func main() {
    // Initialize DB connection.
    initDB()
    defer db.Close()

    r := mux.NewRouter()

    r.HandleFunc("/check", checkServer).Methods("GET")

    r.HandleFunc("/book", createBookHandler).Methods("POST")
    r.HandleFunc("/book/{id}", updateBookHandler).Methods("PUT")

    r.HandleFunc("/books", getAllBooksHandler).Methods("GET")
    r.HandleFunc("/books", deleteAllBooks).Methods("DELETE")


    // Start server.
    log.Printf("Server starting on port 8080:")
    log.Fatal(http.ListenAndServe(":8080", r))
}
