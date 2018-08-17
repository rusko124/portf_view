# coding: utf-8
from PyQt5.QtCore import QPoint, Qt
from PyQt5.QtGui import QKeyEvent
from PyQt5.QtWebEngineWidgets import QWebEnginePage, QWebEngineView
from PyQt5.QtWidgets import QApplication, QWidget

from .white_diagnostic import WhiteDiagnosticMessage


class WebView(QWebEngineView):

    def convert_position(self, point):
        for child in self.children():
            if isinstance(child, QWidget):
                return child.mapToGlobal(point)

    def send_event(self, event):
        for child in self.children():
            if isinstance(child, QWidget):
                QApplication.sendEvent(child, event)

    def send_keyboard_event(self, event, key):
        event = QKeyEvent(event, key, Qt.NoModifier)
        self.send_event(event)


class WebPage(QWebEnginePage):

    def __init__(self, *args, bot, **kwargs):
        self.bot = bot
        super().__init__(*args, **kwargs)

    def javaScriptConsoleMessage(self, level, msg, lineNumber, sourceID):
        if msg.startswith('WDDEBUG: '):
            if self.bot.wd_debug:
                message = WhiteDiagnosticMessage(msg[9:])
                self.bot.logger.log(message.formatted)
                self.bot.logger.log(message.important_info)
        else:
            self.bot.logger.log('JS - [%s:%d]: %s' % (sourceID, lineNumber, msg), status='?')


class Link:

    def __init__(self, cursor, url, position, size, visibleArea):
        self.cursor = cursor
        self.url = url
        self.position = position
        self.size = size

    def __str__(self):
        return self.url or ''

    def __repr__(self):
        return '<Link: %s>' % self

    def __hash__(self):
        return hash(self.url)

    @property
    def point(self):
        return QPoint(self.position['x'], self.position['y'])

    def __eq__(self, other):
        return self.url == other.url

    def click(self):
        self.cursor.click(self.point)
