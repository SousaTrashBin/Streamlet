package urb;

import utils.application.Message;

public interface URBCallback {
    void onDelivery(Message message);
}
