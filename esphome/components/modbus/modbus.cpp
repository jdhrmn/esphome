#include "modbus.h"
#include "esphome/core/log.h"
#include "esphome/core/helpers.h"

namespace esphome {
namespace modbus {

static const char *const TAG = "modbus";

void Modbus::setup() {
  if (this->flow_control_pin_ != nullptr) {
    this->flow_control_pin_->setup();
  }
}
void Modbus::loop() {
  const uint32_t now = millis();

  while (this->available()) {
    uint8_t byte;
    this->read_byte(&byte);
    if (this->parse_rx_byte_(byte)) {
      this->last_modbus_byte_ = now;
    } else {
      size_t at = this->rx_buffer_.size();
      if (at > 0) {
        ESP_LOGV(TAG, "Clearing buffer of %d bytes - parse failed", at);
        this->rx_buffer_.clear();
      }
      this->rx_length_ascii_ = 0;
    }
  }

  if (now - this->last_modbus_byte_ > this->rx_character_timeout) {
    size_t at = this->rx_buffer_.size();
    if (at > 0) {
      ESP_LOGV(TAG, "Clearing buffer of %d bytes - timeout", at);
      this->rx_buffer_.clear();
    }
    this->rx_length_ascii_ = 0;

    // stop blocking new send commands after sent_wait_time_ ms after response received
    if (now - this->last_send_ > send_wait_time_) {
      if (waiting_for_response > 0) {
        ESP_LOGV(TAG, "Stop waiting for response from %d", waiting_for_response);
      }
      waiting_for_response = 0;
    }
  }
}

bool Modbus::parse_rx_byte_(uint8_t byte) {
  switch (this->transmission_mode) {
    case ModbusTransmissionMode::ASCII: {
      size_t at = this->rx_length_ascii_;
      this->rx_length_ascii_ += 1;
      ESP_LOGVV(TAG, "Modbus ASCII received Char  %c (0X%x)", byte, byte);

      // Byte 0: Start of Frame character
      if (at == 0) {
        if (byte == this->ascii_rx_start_of_frame_) {
          return true;
        } else {
          ESP_LOGW(TAG, "Modbus ASCII received character '%c'(0X%x) is not Start-of-frame character '%c'(0X%x)", byte,
                   byte, this->ascii_rx_start_of_frame_, this->ascii_rx_start_of_frame_);
          return false;
        }
      }

      if ((at % 2) != 0) {
        this->rx_byte_ascii[0] = byte;
        return true;  // Char count not even
      } else {
        this->rx_byte_ascii[1] = byte;
      }

      if ((this->rx_byte_ascii[0] == '\r') && (this->rx_byte_ascii[1] == '\n')) {
        ESP_LOGVV(TAG, "Modbus ASCII received End-of-frame (CRLF)");
        return false;
      }

      uint8_t converted_byte = static_cast<uint8_t>(strtol(this->rx_byte_ascii, nullptr, 16));
      return this->parse_modbus_byte_(converted_byte);

      break;
    }
    case ModbusTransmissionMode::RTU: {
      return this->parse_modbus_byte_(byte);
      break;
    }
  }
  return false;
}

bool Modbus::parse_modbus_byte_(uint8_t byte) {
  size_t at = this->rx_buffer_.size();
  this->rx_buffer_.push_back(byte);
  const uint8_t *raw = &this->rx_buffer_[0];
  ESP_LOGVV(TAG, "Modbus received Byte  %d (0X%x)", byte, byte);
  // Byte 0: modbus address (match all)
  if (at == 0)
    return true;
  uint8_t address = raw[0];
  uint8_t function_code = raw[1];
  // Byte 2: Size (with modbus rtu function code 4/3)
  // See also https://en.wikipedia.org/wiki/Modbus
  if (at == 2)
    return true;

  uint8_t data_len = raw[2];
  uint8_t data_offset = 3;

  // Per https://modbus.org/docs/Modbus_Application_Protocol_V1_1b3.pdf Ch 5 User-Defined function codes
  if (((function_code >= 65) && (function_code <= 72)) || ((function_code >= 100) && (function_code <= 110))) {
    // Handle user-defined function, since we don't know how big this ought to be,
    // ideally we should delegate the entire length detection to whatever handler is
    // installed, but wait, there is the CRC, and if we get a hit there is a good
    // chance that this is a complete message ... admittedly there is a small chance is
    // isn't but that is quite small given the purpose of the CRC in the first place

    // Fewer than 2 bytes can't calc CRC
    if (at < 2)
      return true;

    data_len = at - 2;
    data_offset = 1;
    uint16_t computed_checksum = 0;
    uint16_t remote_checksum = 0;
    switch (this->transmission_mode) {
      case ModbusTransmissionMode::ASCII: {
        computed_checksum = lrc(raw, data_offset + data_len);
        remote_checksum = uint16_t(raw[data_offset + data_len]);
        break;
      }
      case ModbusTransmissionMode::RTU: {
        computed_checksum = crc16(raw, data_offset + data_len);
        remote_checksum = uint16_t(raw[data_offset + data_len]) | (uint16_t(raw[data_offset + data_len + 1]) << 8);
        break;
      }
    }

    if (computed_checksum != remote_checksum)
      return true;

    ESP_LOGD(TAG, "Modbus user-defined function %02X found", function_code);

  } else {
    // data starts at 2 and length is 4 for read registers commands
    if (this->role == ModbusRole::SERVER && (function_code == 0x3 || function_code == 0x4)) {
      data_offset = 2;
      data_len = 4;
    }

    // the response for write command mirrors the requests and data starts at offset 2 instead of 3 for read commands
    if (function_code == 0x5 || function_code == 0x06 || function_code == 0xF || function_code == 0x10) {
      data_offset = 2;
      data_len = 4;
    }

    // Error ( msb indicates error )
    // response format:  Byte[0] = device address, Byte[1] function code | 0x80 , Byte[2] exception code, Byte[3-4] crc
    if ((function_code & 0x80) == 0x80) {
      data_offset = 2;
      data_len = 1;
    }

    // Byte data_offset..data_offset+data_len-1: Data
    if (at < data_offset + data_len)
      return true;

    switch (this->transmission_mode) {
      case ModbusTransmissionMode::ASCII: {
        // Byte data_offset+len: LRC (over all bytes)
        uint16_t computed_lrc = lrc(raw, data_offset + data_len);
        uint16_t remote_lrc = uint16_t(raw[data_offset + data_len]);
        if (computed_lrc != remote_lrc) {
          if (this->disable_crc_) {
            ESP_LOGD(TAG, "Modbus ASCII LRC Check failed, but ignored! %02X!=%02X", computed_lrc, remote_lrc);
          } else {
            ESP_LOGW(TAG, "Modbus ASCII LRC Check failed! %02X!=%02X", computed_lrc, remote_lrc);
            return false;
          }
        }
        break;
      }
      case ModbusTransmissionMode::RTU: {
        // Byte 3+data_len: CRC_LO (over all bytes)
        if (at == data_offset + data_len)
          return true;

        // Byte data_offset+len+1: CRC_HI (over all bytes)
        uint16_t computed_crc = crc16(raw, data_offset + data_len);
        uint16_t remote_crc = uint16_t(raw[data_offset + data_len]) | (uint16_t(raw[data_offset + data_len + 1]) << 8);
        if (computed_crc != remote_crc) {
          if (this->disable_crc_) {
            ESP_LOGD(TAG, "Modbus CRC Check failed, but ignored! %02X!=%02X", computed_crc, remote_crc);
          } else {
            ESP_LOGW(TAG, "Modbus CRC Check failed! %02X!=%02X", computed_crc, remote_crc);
            return false;
          }
        }
        break;
      }
    }
  }
  std::vector<uint8_t> data(this->rx_buffer_.begin() + data_offset, this->rx_buffer_.begin() + data_offset + data_len);
  bool found = false;
  for (auto *device : this->devices_) {
    if (device->address_ == address) {
      // Is it an error response?
      if ((function_code & 0x80) == 0x80) {
        ESP_LOGD(TAG, "Modbus error function code: 0x%X exception: %d", function_code, raw[2]);
        if (waiting_for_response != 0) {
          device->on_modbus_error(function_code & 0x7F, raw[2]);
        } else {
          // Ignore modbus exception not related to a pending command
          ESP_LOGD(TAG, "Ignoring Modbus error - not expecting a response");
        }
      } else if (this->role == ModbusRole::SERVER && (function_code == 0x3 || function_code == 0x4)) {
        device->on_modbus_read_registers(function_code, uint16_t(data[1]) | (uint16_t(data[0]) << 8),
                                         uint16_t(data[3]) | (uint16_t(data[2]) << 8));
      } else {
        device->on_modbus_data(data);
      }
      found = true;
    }
  }
  waiting_for_response = 0;

  if (!found) {
    ESP_LOGW(TAG, "Got Modbus frame from unknown address 0x%02X! ", address);
  }

  // reset buffer
  ESP_LOGV(TAG, "Clearing buffer of %d bytes - parse succeeded", at);
  this->rx_buffer_.clear();
  return true;
}

void Modbus::write_byte_as_ascii_(uint8_t byte) {
  char hex_char[3];
  sprintf(hex_char, "%02X", byte);
  this->write_byte(hex_char[0]);
  this->write_byte(hex_char[1]);
}

void Modbus::write_array_as_ascii_(const std::vector<uint8_t> &data) {
  for (int i = 0; i < data.size(); i++) {
    this->write_byte_as_ascii_(data[i]);
  }
}

void Modbus::dump_config() {
  ESP_LOGCONFIG(TAG, "Modbus:");
  LOG_PIN("  Flow Control Pin: ", this->flow_control_pin_);
  ESP_LOGCONFIG(TAG, "  Send Wait Time: %d ms", this->send_wait_time_);
  ESP_LOGCONFIG(TAG, "  CRC Disabled: %s", YESNO(this->disable_crc_));
  ESP_LOGCONFIG(TAG, "  RX Character Timeout: %d ms", this->rx_character_timeout);
  switch (this->transmission_mode) {
    case ModbusTransmissionMode::ASCII: {
      ESP_LOGCONFIG(TAG, "  Transmission Mode: ASCII");
      ESP_LOGCONFIG(TAG, "  ASCII RX Start-of-frame: %c", this->ascii_rx_start_of_frame_);
      ESP_LOGCONFIG(TAG, "  ASCII TX Start-of-frame: %c", this->ascii_tx_start_of_frame_);
      break;
    }
    case ModbusTransmissionMode::RTU: {
      ESP_LOGCONFIG(TAG, "  Transmission Mode: RTU");
      break;
    }
  }
}
float Modbus::get_setup_priority() const {
  // After UART bus
  return setup_priority::BUS - 1.0f;
}

void Modbus::send(uint8_t address, uint8_t function_code, uint16_t start_address, uint16_t number_of_entities,
                  uint8_t payload_len, const uint8_t *payload) {
  static const size_t MAX_VALUES = 128;

  // Only check max number of registers for standard function codes
  // Some devices use non standard codes like 0x43
  if (number_of_entities > MAX_VALUES && function_code <= 0x10) {
    ESP_LOGE(TAG, "send too many values %d max=%zu", number_of_entities, MAX_VALUES);
    return;
  }

  std::vector<uint8_t> data;
  data.push_back(address);
  data.push_back(function_code);
  if (this->role == ModbusRole::CLIENT) {
    data.push_back(start_address >> 8);
    data.push_back(start_address >> 0);
    if (function_code != 0x5 && function_code != 0x6) {
      data.push_back(number_of_entities >> 8);
      data.push_back(number_of_entities >> 0);
    }
  }

  if (payload != nullptr) {
    if (this->role == ModbusRole::SERVER || function_code == 0xF || function_code == 0x10) {  // Write multiple
      data.push_back(payload_len);  // Byte count is required for write
    } else {
      payload_len = 2;  // Write single register or coil
    }
    for (int i = 0; i < payload_len; i++) {
      data.push_back(payload[i]);
    }
  }

  if (this->flow_control_pin_ != nullptr)
    this->flow_control_pin_->digital_write(true);

  switch (this->transmission_mode) {
    case ModbusTransmissionMode::ASCII: {
      auto checksum = lrc(data.data(), data.size());
      data.push_back(checksum);

      this->write_byte(this->ascii_tx_start_of_frame_);
      this->write_array_as_ascii_(data);
      this->write_byte('\r');
      this->write_byte('\n');
      break;
    }
    case ModbusTransmissionMode::RTU: {
      auto crc = crc16(data.data(), data.size());
      data.push_back(crc >> 0);
      data.push_back(crc >> 8);

      this->write_array(data);
      break;
    }
  }

  this->flush();

  if (this->flow_control_pin_ != nullptr)
    this->flow_control_pin_->digital_write(false);
  waiting_for_response = address;
  last_send_ = millis();
  ESP_LOGV(TAG, "Modbus write: %s", format_hex_pretty(data).c_str());
}

// Helper function for lambdas
// Send raw command. Except CRC everything must be contained in payload
void Modbus::send_raw(const std::vector<uint8_t> &payload) {
  if (payload.empty()) {
    return;
  }

  if (this->flow_control_pin_ != nullptr)
    this->flow_control_pin_->digital_write(true);

  switch (this->transmission_mode) {
    case ModbusTransmissionMode::ASCII: {
      auto checksum = lrc(payload.data(), payload.size());
      this->write_byte(this->ascii_tx_start_of_frame_);
      this->write_array_as_ascii_(payload);
      this->write_byte_as_ascii_(checksum);
      this->write_byte('\r');
      this->write_byte('\n');
      break;
    }
    case ModbusTransmissionMode::RTU: {
      auto crc = crc16(payload.data(), payload.size());
      this->write_array(payload);
      this->write_byte(crc & 0xFF);
      this->write_byte((crc >> 8) & 0xFF);
      break;
    }
  }

  this->flush();
  if (this->flow_control_pin_ != nullptr)
    this->flow_control_pin_->digital_write(false);
  waiting_for_response = payload[0];
  ESP_LOGV(TAG, "Modbus write raw: %s", format_hex_pretty(payload).c_str());
  last_send_ = millis();
}

}  // namespace modbus
}  // namespace esphome
