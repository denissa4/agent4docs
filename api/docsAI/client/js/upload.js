const token = sessionStorage.getItem("apiToken");

async function validate() {
    if (!token) {
        window.location.href = "auth.html";
    } else {
        try {
            const response = await fetch("https://api.nlsql.com/v1/data-source/", {
                method: "GET",
                headers: {
                    Authorization: "Token " + token,
                },
            });

            if (response.ok && (await response.text()) !== "not authorised") {
                sessionStorage.setItem("apiToken", token);
            } else {
                alert("Access denied. Please enter a valid API token");
                window.location.href = "auth.html";
            }
        } catch (error) {
            console.error("Error validating token:", error);
            alert("Failed to validate token. Please try again.");
            window.location.href = "auth.html";
        }
    }
}

async function uploadFiles(event) {
    event.preventDefault();
    const el = document.getElementById("file-upload");
    const files = el.files;

    const allowedTypes = ["application/pdf", "text/plain", "application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"];
    const formData = new FormData();
    const feedback = document.getElementById("feedback");

    feedback.innerHTML = "";

    for (const file of files) {
        if (allowedTypes.includes(file.type)) {
            formData.append("files[]", file);
        } else {
            alert(`File type not allowed: ${file.type}. Please upload a PDF, TXT, or Word document.`);
        }
    }

    const response = await fetch("http://localhost:8000/upload", {
        method: "POST",
        body: formData,
        // mode: "no-cors",
    })
        .then((response) => {
            if (!response.ok) {
                throw new Error("Network response was not ok " + response.statusText);
            }
            return response.json(); // Parse the response as JSON
        })
        .then((data) => {
            console.log("Success:", data); // Log the response data
            feedback.innerHTML = "Your documents have been uploaded successfully.";
            clearFeedback();
        })
        .catch((error) => {
            console.error("Error:", error); // Log any errors
            feedback.innerHTML = "There was an error uploading your documents.";
            clearFeedback();
        });
}

function clearFeedback() {
    setTimeout(() => {
        feedback.innerHTML = "";
    }, 5000);
}

// Call validate() on page load to verify user's API token
validate();
