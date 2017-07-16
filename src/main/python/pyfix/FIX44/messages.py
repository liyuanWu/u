from pyfix.FIX44 import msgtype, fixtags
from pyfix.message import FIXMessage

class Messages(object):

    USERNAME = 'fcd8af68-2f1e-4d9e-816f-15c91b42474c'
    PASSWORD = '90ec6db50fde54c10e537e562c438700'

    @staticmethod
    def logon(senderCompID, targetCompID):
        msg = FIXMessage(msgtype.LOGON)
        msg.setField(fixtags.SenderCompID, senderCompID)
        msg.setField(fixtags.TargetCompID, targetCompID)
        msg.setField(fixtags.Username, Messages.USERNAME)
        msg.setField(fixtags.Password, Messages.PASSWORD)
        return msg

    @staticmethod
    def logout():
        msg = FIXMessage(msgtype.LOGOUT)
        return msg

    @staticmethod
    def heartbeat():
        msg = FIXMessage(msgtype.HEARTBEAT)
        return msg

    @staticmethod
    def test_request():
        msg = FIXMessage(msgtype.TESTREQUEST)
        return msg

    @staticmethod
    def sequence_reset(respondingTo, isGapFill):
        msg = FIXMessage(msgtype.SEQUENCERESET)
        msg.setField(fixtags.GapFillFlag, 'Y' if isGapFill else 'N')
        msg.setField(fixtags.MsgSeqNum, respondingTo[fixtags.BeginSeqNo])
        return msg
    #
    # @staticmethod
    # def sequence_reset(beginSeqNo, endSeqNo, isGapFill):
    #     msg = FIXMessage(msgtype.SEQUENCERESET)
    #     msg.setField(fixtags.GapFillFlag, 'Y' if isGapFill else 'N')
    #     msg.setField(fixtags.MsgSeqNum, respondingTo[fixtags.BeginSeqNo])
    #     return msg


    @staticmethod
    def resend_request(beginSeqNo, endSeqNo = '0'):
        msg = FIXMessage(msgtype.RESENDREQUEST)
        msg.setField(fixtags.BeginSeqNo, str(beginSeqNo))
        msg.setField(fixtags.EndSeqNo, str(endSeqNo))
        return msg