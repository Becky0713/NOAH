#!/bin/bash

# NYC Housing Hub - One-Click Deployment Script
# Supports multiple deployment environments

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to setup environment
setup_environment() {
    print_status "Setting up environment..."
    
    if [ ! -f ".env" ]; then
        if [ -f "env.example" ]; then
            cp env.example .env
            print_success "Created .env file from template"
            print_warning "Please edit .env file with your configuration"
        else
            print_error "No env.example file found"
            exit 1
        fi
    else
        print_status ".env file already exists"
    fi
}

# Function to deploy with Docker
deploy_docker() {
    print_status "Deploying with Docker..."
    
    if ! command_exists docker; then
        print_error "Docker is not installed"
        exit 1
    fi
    
    if ! command_exists docker-compose; then
        print_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Build and start services
    docker-compose up -d --build
    
    print_success "Docker deployment completed"
    print_status "Backend: http://localhost:8000"
    print_status "Frontend: http://localhost:8501"
}

# Function to deploy to Railway
deploy_railway() {
    print_status "Deploying to Railway..."
    
    if ! command_exists railway; then
        print_error "Railway CLI is not installed"
        print_status "Install it from: https://docs.railway.app/develop/cli"
        exit 1
    fi
    
    railway login
    railway link
    railway up
    
    print_success "Railway deployment completed"
}

# Function to deploy to Render
deploy_render() {
    print_status "Deploying to Render..."
    print_status "Please visit: https://render.com"
    print_status "1. Connect your GitHub account"
    print_status "2. Select 'New Web Service'"
    print_status "3. Choose your repository: Becky0713/NOAH"
    print_status "4. Use these settings:"
    print_status "   - Build Command: pip install -r requirements.txt"
    print_status "   - Start Command: uvicorn backend.main:app --host 0.0.0.0 --port \$PORT"
    print_status "   - Environment: Python 3"
    print_status "5. Add environment variables:"
    print_status "   - DATA_PROVIDER=socrata"
    print_status "   - HOST=0.0.0.0"
    print_status "   - PORT=10000"
    print_success "Render deployment instructions provided"
}

# Function to deploy to Heroku
deploy_heroku() {
    print_status "Deploying to Heroku..."
    
    if ! command_exists heroku; then
        print_error "Heroku CLI is not installed"
        print_status "Install it from: https://devcenter.heroku.com/articles/heroku-cli"
        exit 1
    fi
    
    heroku create
    git push heroku main
    
    print_success "Heroku deployment completed"
}

# Function to deploy to Streamlit Cloud
deploy_streamlit() {
    print_status "Deploying to Streamlit Cloud..."
    print_status "Please visit: https://share.streamlit.io"
    print_status "1. Connect your GitHub repository"
    print_status "2. Set main file to: frontend/app.py"
    print_status "3. Add environment variable: BACKEND_URL"
    print_success "Streamlit Cloud deployment instructions provided"
}

# Main deployment function
main() {
    echo "🏠 NYC Housing Hub - Deployment Script"
    echo "======================================"
    
    # Check if we're in the right directory
    if [ ! -f "requirements.txt" ]; then
        print_error "Please run this script from the project root directory"
        exit 1
    fi
    
    # Setup environment
    setup_environment
    
    # Show deployment options
    echo ""
    echo "Select deployment method:"
    echo "1) Docker (Local)"
    echo "2) Railway (Backend - 30 days free)"
    echo "3) Render (Backend - 750 hours/month FREE)"
    echo "4) Heroku (Full Stack - 550-1000 hours/month FREE)"
    echo "5) Streamlit Cloud (Frontend - PERMANENT FREE)"
    echo "6) All Free (Render + Streamlit)"
    echo ""
    
    read -p "Enter your choice (1-6): " choice
    
    case $choice in
        1)
            deploy_docker
            ;;
        2)
            deploy_railway
            ;;
        3)
            deploy_render
            ;;
        4)
            deploy_heroku
            ;;
        5)
            deploy_streamlit
            ;;
        6)
            deploy_render
            deploy_streamlit
            ;;
        *)
            print_error "Invalid choice"
            exit 1
            ;;
    esac
    
    echo ""
    print_success "Deployment completed!"
    echo ""
    echo "📚 Next steps:"
    echo "1. Check the deployment logs"
    echo "2. Test the application"
    echo "3. Configure environment variables"
    echo "4. Set up monitoring"
    echo ""
    echo "📖 For more information, see:"
    echo "- README.md - Project overview"
    echo "- MIGRATION_GUIDE.md - Environment migration"
    echo "- DEPLOYMENT.md - Detailed deployment guide"
}

# Run main function
main "$@"
