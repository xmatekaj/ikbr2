"""
Notification system for the trading bot.
Handles sending alerts through various channels (email, SMS, etc.).
"""
import logging
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional, Union

# Set up logger
logger = logging.getLogger(__name__)

class Notifier:
    """
    Handles sending notifications through various channels.
    Currently supports email notifications.
    """
    
    def __init__(self, config=None):
        """
        Initialize the notifier with configuration.
        
        Args:
            config: Configuration dictionary with notification settings
        """
        self.config = config or {}
        self.email_config = self.config.get('email', {})
        self.sms_config = self.config.get('sms', {})
        
        # Initialize notification channels
        self._setup_email()
    
    def _setup_email(self):
        """Set up email notification channel."""
        self.email_enabled = self.email_config.get('enabled', False)
        
        if self.email_enabled:
            self.smtp_server = self.email_config.get('smtp_server')
            self.smtp_port = self.email_config.get('smtp_port', 587)
            self.smtp_username = self.email_config.get('username')
            self.smtp_password = self.email_config.get('password')
            self.sender_email = self.email_config.get('sender', self.smtp_username)
            self.recipients = self.email_config.get('recipients', [])
            
            # Validate email configuration
            if not all([self.smtp_server, self.smtp_username, 
                       self.smtp_password, self.recipients]):
                logger.warning("Email notifications enabled but configuration incomplete")
                self.email_enabled = False
    
    def send_notification(self, 
                        message: str, 
                        subject: str = "Trading Bot Alert",
                        importance: str = "normal",
                        channels: List[str] = None) -> Dict[str, bool]:
        """
        Send a notification via configured channels.
        
        Args:
            message: The notification message
            subject: Subject line for the notification
            importance: Importance level ('low', 'normal', 'high', 'critical')
            channels: List of channels to use (default: all enabled channels)
            
        Returns:
            Dictionary with status of each channel attempt
        """
        if channels is None:
            channels = ['email', 'sms']
        
        results = {}
        
        if 'email' in channels and self.email_enabled:
            results['email'] = self.send_email(subject, message, importance)
        
        if 'sms' in channels and getattr(self, 'sms_enabled', False):
            results['sms'] = self.send_sms(message, importance)
        
        return results
    
    def send_email(self, 
                 subject: str, 
                 message: str, 
                 importance: str = "normal") -> bool:
        """
        Send an email notification.
        
        Args:
            subject: Email subject
            message: Email body
            importance: Importance level affecting email priority
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.email_enabled:
            logger.warning("Email notifications not properly configured")
            return False
        
        try:
            # Create message
            email = MIMEMultipart()
            email['From'] = self.sender_email
            email['To'] = ", ".join(self.recipients)
            email['Subject'] = subject
            
            # Set priority based on importance
            if importance == 'high':
                email['X-Priority'] = '2'
            elif importance == 'critical':
                email['X-Priority'] = '1'
            
            # Attach message
            email.attach(MIMEText(message, 'plain'))
            
            # Connect to server
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(email)
            
            logger.info(f"Email notification sent: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    def send_sms(self, 
               message: str, 
               importance: str = "normal") -> bool:
        """
        Send an SMS notification.
        
        Args:
            message: SMS content
            importance: Importance level (may affect delivery)
            
        Returns:
            True if sent successfully, False otherwise
        """
        # This is a placeholder for SMS functionality
        # You would need to implement this using a service like Twilio
        logger.warning("SMS notifications not implemented")
        return False

    def configure_email(self, 
                      smtp_server: str, 
                      username: str, 
                      password: str,
                      recipients: List[str],
                      port: int = 587,
                      sender: str = None) -> bool:
        """
        Configure email notifications.
        
        Args:
            smtp_server: SMTP server address
            username: SMTP username
            password: SMTP password
            recipients: List of email recipients
            port: SMTP port (default: 587 for TLS)
            sender: Sender email (default: same as username)
            
        Returns:
            True if configuration successful
        """
        self.smtp_server = smtp_server
        self.smtp_port = port
        self.smtp_username = username
        self.smtp_password = password
        self.sender_email = sender or username
        self.recipients = recipients
        
        # Update configuration dictionary
        self.email_config = {
            'enabled': True,
            'smtp_server': smtp_server,
            'smtp_port': port,
            'username': username,
            'password': password,
            'sender': self.sender_email,
            'recipients': recipients
        }
        
        self.config['email'] = self.email_config
        self.email_enabled = True
        
        return True