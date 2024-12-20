#pragma once

#include "esphome/core/component.h"
#include "esphome/components/uart/uart.h"

#include <vector>

namespace esphome {
namespace modbus {

enum ModbusRole {
  CLIENT,
  SERVER,
};

enum ModbusTransmissionMode {
  RTU,
  ASCII,
};

class ModbusDevice;

class Modbus : public uart::UARTDevice, public Component {
 public:
  Modbus() = default;

  void setup() override;

  void loop() override;

  void dump_config() override;

  void register_device(ModbusDevice *device) { this->devices_.push_back(device); }

  float get_setup_priority() const override;

  void send(uint8_t address, uint8_t function_code, uint16_t start_address, uint16_t number_of_entities,
            uint8_t payload_len = 0, const uint8_t *payload = nullptr);
  void send_raw(const std::vector<uint8_t> &payload);
  void set_role(ModbusRole role) { this->role = role; }
  void set_transmission_mode(ModbusTransmissionMode transmission_mode) { this->transmission_mode = transmission_mode; }
  void set_ascii_rx_start_of_frame(char ascii_rx_start_of_frame) {
    this->ascii_rx_start_of_frame_ = ascii_rx_start_of_frame;
  }
  void set_ascii_tx_start_of_frame(char ascii_tx_start_of_frame) {
    this->ascii_tx_start_of_frame_ = ascii_tx_start_of_frame;
  }
  void set_rx_character_timeout(uint16_t time_in_ms) { rx_character_timeout = time_in_ms; }
  void set_flow_control_pin(GPIOPin *flow_control_pin) { this->flow_control_pin_ = flow_control_pin; }
  uint8_t waiting_for_response{0};
  void set_send_wait_time(uint16_t time_in_ms) { send_wait_time_ = time_in_ms; }
  void set_disable_crc(bool disable_crc) { disable_crc_ = disable_crc; }

  ModbusRole role;
  ModbusTransmissionMode transmission_mode;

 protected:
  GPIOPin *flow_control_pin_{nullptr};

  bool parse_modbus_byte_(uint8_t byte);
  bool parse_rx_byte_(uint8_t byte);
  void write_byte_as_ascii_(uint8_t byte);
  void write_array_as_ascii_(const std::vector<uint8_t> &data);
  uint16_t send_wait_time_{250};
  char ascii_rx_start_of_frame_{':'};
  char ascii_tx_start_of_frame_{':'};
  uint16_t rx_character_timeout{50};
  bool disable_crc_;
  std::vector<uint8_t> rx_buffer_;
  uint16_t rx_length_ascii_{0};
  char rx_byte_ascii[3]{0};
  uint32_t last_modbus_byte_{0};
  uint32_t last_send_{0};
  std::vector<ModbusDevice *> devices_;
  uint8_t lrc(const uint8_t *data, uint16_t len);
};

class ModbusDevice {
 public:
  void set_parent(Modbus *parent) { parent_ = parent; }
  void set_address(uint8_t address) { address_ = address; }
  virtual void on_modbus_data(const std::vector<uint8_t> &data) = 0;
  virtual void on_modbus_error(uint8_t function_code, uint8_t exception_code) {}
  virtual void on_modbus_read_registers(uint8_t function_code, uint16_t start_address, uint16_t number_of_registers){};
  void send(uint8_t function, uint16_t start_address, uint16_t number_of_entities, uint8_t payload_len = 0,
            const uint8_t *payload = nullptr) {
    this->parent_->send(this->address_, function, start_address, number_of_entities, payload_len, payload);
  }
  void send_raw(const std::vector<uint8_t> &payload) { this->parent_->send_raw(payload); }
  // If more than one device is connected block sending a new command before a response is received
  bool waiting_for_response() { return parent_->waiting_for_response != 0; }

 protected:
  friend Modbus;

  Modbus *parent_;
  uint8_t address_;
};

}  // namespace modbus
}  // namespace esphome
