# RAG Support Chatbot

A Retrieval-Augmented Generation (RAG) powered chatbot that provides instant answers using structured and scraped data. Built with FastAPI, OpenAI GPT, and a custom web scraper.

## Table of Contents
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Future Improvements](#future-improvements)
- [Demo Video](#demo-video)
- [License](#license)
- [Contributing](#contributing)

## Features
- Website scraping to extract text, pricing, contacts, teams, and images.
- Knowledge base stored as JSONL files and embeddings for efficient retrieval.
- RAG-powered chatbot combining structured data with context-aware LLM responses.
- Optional WhatsApp-style training using exported chats to fine-tune conversational style.
- Simple frontend chat interface with continuous conversation support.

## Demo Video

(https://drive.google.com/file/d/1MyXYNt1sIEkvzdXGJJ6kYqP7LIx4QH2O/view?usp=sharing)


## Project Structure

```
SCRAPPER/
├── chatbot/                     # Knowledge base for chatbot
│   ├── structured/              # Extracted structured data
│   │   ├── contacts.json        # Contact information
│   │   ├── locations.json       # Location details
│   │   ├── prices.json          # Pricing information
│   │   ├── teams.json           # Team details
│   ├── chunks.jsonl             # Scraped content chunks
│   ├── state.json               # Embedding and index state
├── data/                        # Raw scraped data
│   ├── images/                  # Downloaded images
│   ├── chunks.jsonl             # Raw scraped content
│   ├── pages.json               # Scraped page data
│   ├── images.csv               # Image metadata
├── make_chatbot_views.py        # Converts scraped data to structured views
├── scrape_zirmon.py             # Example scraper script
├── requirements.txt             # Python dependencies
```

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/taniajasmin/RAG-Support-Chatbot.git
   cd RAG-Support-Chatbot
   ```

2. **Create a Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set Environment Variables**:
   Create a `.env` file in the project root with the following:
   ```ini
   OPENAI_API_KEY=your_openai_key_here
   CHAT_MODEL=gpt-4o-mini
   EMBED_MODEL=text-embedding-3-small
   ```

## Usage

1. **Run the Scraper**:
   ```bash
   python scrape_zirmon.py
   python make_chatbot_views.py
   ```

2. **Start the API**:
   ```bash
   uvicorn app.main:app --reload
   ```

3. **Access the Chatbot**:
   Navigate to `http://127.0.0.1:8000` in your browser.

4. **Query the Chatbot**:
   Ask about pricing, contacts, teams, locations, or services. The chatbot supports continuous conversations for back-and-forth queries.

## Future Improvements
- Integrate WhatsApp-style training data for enhanced conversational style.
- Add a vector database (e.g., Pinecone, Weaviate) for large-scale retrieval.
- Enhance the UI with buttons and quick action features.

## License

This project is licensed under the MIT License. You are free to use, modify, and distribute the code as per the license terms.

## Contributing

Contributions are welcome. To contribute:
1. Open an issue to discuss proposed changes or features.
2. Submit pull requests with clear descriptions of your changes.
