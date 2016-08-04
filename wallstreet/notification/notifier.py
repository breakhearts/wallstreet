from mailthon import postman, email
from wallstreet import config


class Notifier(object):
    def send_text(self, title, msg):
        """
        :params title:title msg: raw text msg
        """
        raise NotImplementedError


class EmailNotifier(Notifier):
    def __init__(self, host, port, username, password, sender, receivers):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.smtp = postman(host=self.host, port=self.port, auth=(self.username, self.password))
        self.sender = sender
        self.receivers = receivers

    def add_receivers(self, receiver):
        try:
            self.receivers.index(receiver)
        except:
            self.receivers.append(receiver)

    def send_text(self, title, msg):
        self.smtp.send(email(
            sender=self.sender,
            subject=title,
            receivers=self.receivers,
            content=msg
        ))


email_notifier = EmailNotifier(config.get("notifier", "host"), config.get_int("notifier", "port"),
                               config.get("notifier", "username"), config.get("notifier", "password"),
                               config.get("notifier", "sender"), config.get("notifier", "receivers"))